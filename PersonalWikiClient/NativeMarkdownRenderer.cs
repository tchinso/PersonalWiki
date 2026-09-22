using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace PersonalWikiClient;

internal sealed class RendererActions
{
    public Func<string, Task> OpenDocumentAsync { get; init; } = _ => Task.CompletedTask;
    public Func<string, Task> CreateDocumentAsync { get; init; } = _ => Task.CompletedTask;
    public Func<string, Task> OpenExternalAsync { get; init; } = OpenExternalFallbackAsync;
    public Func<string, Task> DownloadFileAsync { get; init; } = _ => Task.CompletedTask;
    public Func<string, CancellationToken, Task<Image?>> LoadLocalImageAsync { get; init; } = (_, _) => Task.FromResult<Image?>(null);
    public Func<Uri, CancellationToken, Task<Image?>> LoadRemoteImageAsync { get; init; } = (_, _) => Task.FromResult<Image?>(null);

    private static Task OpenExternalFallbackAsync(string url)
    {
        if (!TryGetAllowedExternalUri(url, out var target))
        {
            return Task.CompletedTask;
        }

        try
        {
            Process.Start(new ProcessStartInfo(target.AbsoluteUri) { UseShellExecute = true });
        }
        catch (System.ComponentModel.Win32Exception)
        {
            // The caller has no browser association. Keep native rendering usable.
        }

        return Task.CompletedTask;
    }

    /// <summary>Only conventional browser URLs may leave the native client.</summary>
    internal static bool TryGetAllowedExternalUri(string? url, out Uri target)
    {
        target = null!;
        if (string.IsNullOrWhiteSpace(url))
        {
            return false;
        }

        var decoded = url;
        try
        {
            for (var index = 0; index < 3; index++)
            {
                var next = Uri.UnescapeDataString(decoded);
                if (next == decoded)
                {
                    break;
                }

                decoded = next;
            }
        }
        catch (UriFormatException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }

        if (decoded.Any(character => char.IsControl(character))
            || !Uri.TryCreate(decoded, UriKind.Absolute, out var uri)
            || string.IsNullOrWhiteSpace(uri.Host))
        {
            return false;
        }

        if (!uri.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && !uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        target = uri;
        return true;
    }
}

/// <summary>
/// Translates PersonalWiki's JSON AST entirely to regular WinForms controls.
/// </summary>
internal sealed class NativeMarkdownRenderer : UserControl
{
    // A document can contain many image embeds. Per-image byte caps are not
    // sufficient on their own: starting every fetch/decode at once would
    // multiply those temporary buffers. Keep a small, cancellable per-view
    // queue so a large document remains responsive without serializing it.
    private const int MaxConcurrentImageLoads = 4;
    private readonly Panel _scrollHost = new()
    {
        Dock = DockStyle.Fill,
        AutoScroll = true,
        BackColor = Color.FromArgb(255, 250, 240),
    };
    private readonly TableLayoutPanel _body = new()
    {
        AutoSize = true,
        AutoSizeMode = AutoSizeMode.GrowAndShrink,
        ColumnCount = 1,
        Dock = DockStyle.Top,
        Padding = new Padding(22, 18, 22, 28),
        BackColor = Color.Transparent,
    };
    private readonly Dictionary<string, Control> _anchors = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<(FlowLayoutPanel Panel, AstNode Node)> _tocPanels = [];
    // Every rendered document owns its image requests. Navigating away must
    // stop optional image fetch/decode work instead of retaining old document
    // controls, decoded bitmaps, and network buffers until the form closes.
    private CancellationTokenSource _imageLoads = new();
    private readonly SemaphoreSlim _imageLoadSlots = new(MaxConcurrentImageLoads, MaxConcurrentImageLoads);
    private int _pendingImageLoads;
    private int _rendererDisposed;
    private int _imageLoadSlotsDisposed;
    private RendererActions _actions = new();
    private Font _documentFont = new("Segoe UI", 9f, FontStyle.Regular, GraphicsUnit.Point);

    public NativeMarkdownRenderer()
    {
        AutoScaleMode = AutoScaleMode.Dpi;
        _body.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        _scrollHost.Controls.Add(_body);
        Controls.Add(_scrollHost);
    }

    public void Render(IReadOnlyList<AstNode> ast, RendererActions actions, Font documentFont)
    {
        ResetImageLoads();
        SuspendLayout();
        _body.SuspendLayout();
        try
        {
            _actions = actions;
            _documentFont = documentFont;
            _anchors.Clear();
            _tocPanels.Clear();
            DisposeChildControls(_body);
            _body.Controls.Clear();
            _body.RowStyles.Clear();
            _body.RowCount = 0;

            foreach (var node in ast)
            {
                AddBlock(_body, node);
            }

            if (ast.Count == 0)
            {
                AddBlock(_body, new AstNode("paragraph", "표시할 내용이 없습니다.", null, Array.Empty<AstNode>()));
            }

            PopulateTocs();
        }
        finally
        {
            _body.ResumeLayout(true);
            ResumeLayout(true);
        }
    }

    public void ClearDocument()
    {
        CancelImageLoads();
        DisposeChildControls(_body);
        _body.Controls.Clear();
        _anchors.Clear();
        _tocPanels.Clear();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Interlocked.Exchange(ref _rendererDisposed, 1);
            CancelImageLoads();
            _imageLoads.Dispose();
            if (!_body.IsDisposed)
            {
                DisposeChildControls(_body);
                _body.Controls.Clear();
            }

            DisposeImageLoadSlotsWhenIdle();
        }

        base.Dispose(disposing);
    }

    private void AddBlock(TableLayoutPanel host, AstNode node)
    {
        var control = CreateBlock(node);
        if (control is null)
        {
            return;
        }

        control.Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right;
        control.Margin = new Padding(0, 0, 0, 12);
        host.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        host.Controls.Add(control, 0, host.RowCount++);
    }

    private Control? CreateBlock(AstNode node)
    {
        var type = NormalizeType(node.Type);
        return type switch
        {
            "paragraph" or "block_text" => CreateInlineFlow(node.Children.Count > 0 ? node.Children : [new AstNode("text", node.Text, null, [])]),
            "heading" => CreateHeading(node),
            "block_code" or "fenced_code" or "code_block" => CreateCodeBlock(node),
            "block_quote" or "blockquote" => CreateQuote(node),
            "list" or "task_list" => CreateList(node),
            "table" => CreateTable(node),
            "thematic_break" or "hr" => new Panel { Height = 1, Dock = DockStyle.Top, BackColor = Color.FromArgb(220, 212, 196), Margin = new Padding(0, 7, 0, 15) },
            "callout" => CreateCallout(node),
            "folded_template_block" or "folded_template" => CreateFoldedTemplate(node),
            "template_block" or "template" => node.AttributeBool("folded") == true
                ? CreateFoldedTemplate(node)
                : CreateNestedBlocks(node),
            "toc" => CreateTocPlaceholder(node),
            "image" or "image_embed" => CreateImageBlock(node),
            "youtube" or "youtube_embed" => CreateYoutubeCard(node),
            "tag_embed" or "tag_document_list" => CreateTagDocumentList(node),
            "raw_embed" => CreateRawEmbed(node),
            "html_block" or "block_html" => CreateSafeRawText(node),
            "footnotes" => CreateFootnotes(node),
            "footnote_item" or "footnote" => CreateFootnoteItem(node),
            _ when node.Children.Count > 0 => CreateNestedBlocks(node),
            _ when !string.IsNullOrEmpty(node.Text) => CreateInlineFlow([new AstNode("text", node.Text, null, [])]),
            _ => null,
        };
    }

    private Control CreateHeading(AstNode node)
    {
        var level = Math.Clamp(node.AttributeInt("level") ?? 1, 1, 6);
        var text = InlinePlainText(node.Children, node.Text);
        var size = level switch
        {
            1 => _documentFont.Size + 16,
            2 => _documentFont.Size + 10,
            3 => _documentFont.Size + 6,
            4 => _documentFont.Size + 3,
            _ => _documentFont.Size + 1,
        };
        var label = new Label
        {
            AutoSize = true,
            Text = text,
            Font = FontCache.Get(_documentFont.FontFamily.Name, size, FontStyle.Bold),
            ForeColor = Color.FromArgb(26, 26, 26),
            Margin = new Padding(0, level == 1 ? 0 : 10, 0, 3),
        };
        var anchor = node.AttributeString("anchor", "id");
        if (!string.IsNullOrWhiteSpace(anchor))
        {
            _anchors[anchor] = label;
        }

        return label;
    }

    private Control CreateCodeBlock(AstNode node)
    {
        var source = node.Text ?? InlinePlainText(node.Children, null);
        var text = new TextBox
        {
            Multiline = true,
            ReadOnly = true,
            WordWrap = false,
            ScrollBars = ScrollBars.Both,
            Text = source,
            Font = FontCache.Get("Cascadia Code", Math.Max(9, _documentFont.Size - 1), FontStyle.Regular),
            BackColor = Color.FromArgb(24, 31, 31),
            ForeColor = Color.FromArgb(245, 245, 245),
            BorderStyle = BorderStyle.FixedSingle,
            Height = Math.Min(330, Math.Max(58, (source.Count(character => character == '\n') + 2) * 21)),
            Dock = DockStyle.Top,
        };
        return text;
    }

    private Control CreateQuote(AstNode node)
    {
        var outer = new Panel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            BackColor = Color.FromArgb(250, 245, 232),
            Padding = new Padding(13, 10, 10, 8),
            Margin = new Padding(0, 2, 0, 10),
        };
        outer.Paint += (_, eventArgs) =>
        {
            using var brush = new SolidBrush(Color.FromArgb(184, 164, 237));
            eventArgs.Graphics.FillRectangle(brush, 0, 0, 5, outer.Height);
        };
        var nested = CreateBlockTable(node.Children);
        outer.Controls.Add(nested);
        return outer;
    }

    private Control CreateList(AstNode node)
    {
        var list = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 2,
            Dock = DockStyle.Top,
            Margin = new Padding(0, 2, 0, 7),
        };
        list.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        list.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        var ordered = node.AttributeBool("ordered") ?? false;
        var index = node.AttributeInt("start") ?? 1;
        foreach (var item in node.Children.Where(child => NormalizeType(child.Type) is "list_item" or "task_list_item" or "item"))
        {
            var isTask = NormalizeType(item.Type) == "task_list_item" || item.AttributeBool("checked") is not null;
            var marker = isTask
                ? (item.AttributeBool("checked") == true ? "☑" : "☐")
                : ordered ? $"{index}." : "•";
            var markerLabel = new Label
            {
                AutoSize = true,
                Text = marker,
                Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, FontStyle.Bold),
                Margin = new Padding(0, 2, 8, 2),
            };
            var itemBody = CreateBlockTable(item.Children.Count > 0 ? item.Children : [new AstNode("paragraph", item.Text, null, [])]);
            list.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            list.Controls.Add(markerLabel, 0, list.RowCount);
            list.Controls.Add(itemBody, 1, list.RowCount);
            list.RowCount++;
            index++;
        }

        return list;
    }

    private Control CreateTable(AstNode node)
    {
        var rows = FlattenTableRows(node).ToArray();
        if (rows.Length == 0)
        {
            return CreateSafeRawText(new AstNode("text", "[빈 표]", null, []));
        }

        var columns = rows.Max(row => row.Children.Sum(cell => Math.Max(1, cell.AttributeInt("colspan", "col_span") ?? 1)));
        var table = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = columns,
            CellBorderStyle = TableLayoutPanelCellBorderStyle.Single,
            BackColor = Color.FromArgb(220, 214, 198),
            Margin = new Padding(0, 3, 0, 12),
        };
        var widthHints = node.AttributeFloatList("column_widths");
        var totalWidth = widthHints.Count == columns && widthHints.Sum() > 0
            ? widthHints.Sum()
            : columns;
        for (var column = 0; column < columns; column++)
        {
            var width = widthHints.Count == columns
                ? Math.Max(0.001f, widthHints[column]) * 100f / totalWidth
                : 100f / columns;
            table.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, width));
        }

        foreach (var row in rows)
        {
            var rowIndex = table.RowCount;
            table.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            var isHeader = row.Children.Any(cell =>
                    NormalizeType(cell.Type) is "table_head" or "table_header"
                    || cell.AttributeBool("head") == true)
                || (rowIndex == 0 && (NormalizeType(row.Type) is "table_head" or "table_header"));
            var column = 0;
            foreach (var cell in row.Children)
            {
                var content = CreateTableCell(cell, isHeader);
                table.Controls.Add(content, column, rowIndex);
                var span = Math.Clamp(cell.AttributeInt("colspan", "col_span") ?? 1, 1, columns - column);
                if (span > 1)
                {
                    table.SetColumnSpan(content, span);
                }

                column += span;
                if (column >= columns)
                {
                    break;
                }
            }

            table.RowCount++;
        }

        return table;
    }

    private Control CreateTableCell(AstNode cell, bool isHeader)
    {
        var alignment = (cell.AttributeString("align") ?? "left").ToLowerInvariant();
        if (alignment is not ("left" or "center" or "right"))
        {
            alignment = "left";
        }

        var content = CreateInlineFlow(cell.Children.Count > 0
            ? cell.Children
            : [new AstNode("text", cell.Text, null, [])]);
        content.Margin = Padding.Empty;
        content.Padding = Padding.Empty;
        var cellHost = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = alignment == "center" ? 3 : 2,
            RowCount = 1,
            Dock = DockStyle.Fill,
            Padding = new Padding(6, 5, 6, 5),
            BackColor = isHeader ? Color.FromArgb(235, 230, 214) : Color.FromArgb(255, 250, 240),
            Tag = alignment,
        };
        if (alignment == "center")
        {
            cellHost.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50));
            cellHost.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            cellHost.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50));
            cellHost.Controls.Add(content, 1, 0);
        }
        else if (alignment == "right")
        {
            cellHost.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
            cellHost.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            cellHost.Controls.Add(content, 1, 0);
        }
        else
        {
            cellHost.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
            cellHost.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
            cellHost.Controls.Add(content, 0, 0);
        }

        return cellHost;
    }

    private static IEnumerable<AstNode> FlattenTableRows(AstNode table)
    {
        foreach (var child in table.Children)
        {
            var type = NormalizeType(child.Type);
            if (type is "table_row" or "row" or "table_head" or "table_header")
            {
                if (type is "table_head" or "table_header")
                {
                    yield return new AstNode("table_row", null, child.Attributes, child.Children);
                }
                else
                {
                    yield return child;
                }
            }
            else if (type is "table_body" or "tbody" or "thead")
            {
                foreach (var row in FlattenTableRows(child))
                {
                    yield return row;
                }
            }
        }
    }

    private Control CreateCallout(AstNode node)
    {
        var level = (node.AttributeString("level", "kind") ?? "note").ToLowerInvariant();
        var (icon, color) = level switch
        {
            "info" => ("ⓘ", Color.FromArgb(158, 216, 40)),
            "warn" or "warning" => ("⚠", Color.FromArgb(232, 185, 74)),
            "danger" => ("!", Color.FromArgb(239, 68, 68)),
            _ => ("💡", Color.FromArgb(184, 164, 237)),
        };
        var callout = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 2,
            Padding = new Padding(10, 8, 10, 8),
            BackColor = Color.FromArgb(255, 250, 240),
            Margin = new Padding(0, 3, 0, 12),
        };
        callout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        callout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        callout.Paint += (_, eventArgs) =>
        {
            using var pen = new Pen(color, 2);
            eventArgs.Graphics.DrawRectangle(pen, 1, 1, Math.Max(0, callout.Width - 3), Math.Max(0, callout.Height - 3));
        };
        callout.Controls.Add(new Label
        {
            Text = icon,
            AutoSize = true,
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size + 3, FontStyle.Bold),
            ForeColor = color,
            Margin = new Padding(0, 0, 9, 0),
        }, 0, 0);
        var body = node.Children.Count > 0
            ? CreateInlineFlow(node.Children)
            : CreateInlineFlow([new AstNode("text", node.Text, null, [])]);
        callout.Controls.Add(body, 1, 0);
        return callout;
    }

    private Control CreateFoldedTemplate(AstNode node)
    {
        var reference = node.AttributeString("ref", "title") ?? "템플릿";
        var toggle = new Button
        {
            AutoSize = true,
            Text = $"▶ Template: {reference}",
            FlatStyle = FlatStyle.Flat,
            BackColor = Color.FromArgb(245, 240, 224),
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, FontStyle.Bold),
            TextAlign = ContentAlignment.MiddleLeft,
            Dock = DockStyle.Top,
        };
        var content = CreateBlockTable(node.Children);
        content.Visible = false;
        toggle.Click += (_, _) =>
        {
            content.Visible = !content.Visible;
            toggle.Text = $"{(content.Visible ? "▼" : "▶")} Template: {reference}";
        };
        var wrapper = new Panel { AutoSize = true, AutoSizeMode = AutoSizeMode.GrowAndShrink, Dock = DockStyle.Top };
        wrapper.Controls.Add(content);
        wrapper.Controls.Add(toggle);
        return wrapper;
    }

    private Control CreateNestedBlocks(AstNode node) => CreateBlockTable(node.Children.Count > 0
        ? node.Children
        : [new AstNode("paragraph", node.Text, null, [])]);

    private Control CreateTocPlaceholder(AstNode node)
    {
        var panel = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            FlowDirection = FlowDirection.TopDown,
            WrapContents = false,
            Padding = new Padding(12, 9, 12, 9),
            BackColor = Color.FromArgb(250, 245, 232),
            Margin = new Padding(0, 3, 0, 12),
        };
        panel.Controls.Add(new Label
        {
            AutoSize = true,
            Text = "목차",
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, FontStyle.Bold),
        });
        _tocPanels.Add((panel, node));
        return panel;
    }

    private void PopulateTocs()
    {
        foreach (var (panel, node) in _tocPanels)
        {
            var maxLevel = Math.Clamp(node.AttributeInt("max_level", "maxLevel", "level") ?? 2, 1, 6);
            var headingDtos = node.AttributeObjectList("headings");
            var headings = headingDtos.Count > 0
                ? headingDtos.Select(heading => new AstNode(
                    "heading",
                    JsonValue.String(heading, "title", "text"),
                    heading,
                    [])).ToArray()
                : node.AttributeNodeList("headings");
            if (headings.Count == 0)
            {
                headings = _anchors.Select(pair => CreateFallbackHeading(pair.Key, pair.Value.Text)).ToArray();
            }

            foreach (var heading in headings.Where(heading => (heading.AttributeInt("level") ?? 1) <= maxLevel))
            {
                var anchor = heading.AttributeString("anchor", "id");
                if (string.IsNullOrWhiteSpace(anchor) || !_anchors.TryGetValue(anchor, out var target))
                {
                    continue;
                }

                var link = new LinkLabel
                {
                    AutoSize = true,
                    Text = InlinePlainText(heading.Children, heading.Text),
                    Margin = new Padding(Math.Max(0, ((heading.AttributeInt("level") ?? 1) - 1) * 12), 2, 0, 2),
                };
                link.LinkClicked += (_, _) => _scrollHost.ScrollControlIntoView(target);
                panel.Controls.Add(link);
            }
        }
    }

    private static AstNode CreateFallbackHeading(string anchor, string? text)
    {
        using var attributes = JsonDocument.Parse(
            $"{{\"anchor\":{JsonSerializer.Serialize(anchor)},\"level\":1}}");
        return new AstNode("heading", text, attributes.RootElement.Clone(), []);
    }

    private Control CreateImageBlock(AstNode node)
    {
        var panel = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            FlowDirection = FlowDirection.TopDown,
            WrapContents = false,
            Margin = new Padding(0, 4, 0, 12),
        };
        var url = node.AttributeString("url", "src") ?? node.Text ?? string.Empty;
        var alt = node.AttributeString("alt", "title") ?? "이미지";
        var image = new PictureBox
        {
            SizeMode = PictureBoxSizeMode.Zoom,
            BackColor = Color.FromArgb(245, 240, 224),
            Width = Math.Clamp(node.AttributeInt("width") ?? 560, 96, 960),
            Height = Math.Clamp(node.AttributeInt("height") ?? 315, 64, 640),
            AccessibleName = alt,
        };
        panel.Controls.Add(image);
        panel.Controls.Add(new Label { AutoSize = true, Text = alt, ForeColor = Color.DimGray, Font = FontCache.Get(_documentFont.FontFamily.Name, Math.Max(8, _documentFont.Size - 2), FontStyle.Italic) });
        if (Uri.TryCreate(url, UriKind.Absolute, out var remote)
            && (remote.Scheme == Uri.UriSchemeHttp || remote.Scheme == Uri.UriSchemeHttps))
        {
            StartImageLoad(image, remote, _actions.LoadRemoteImageAsync, _imageLoads.Token);
        }
        else
        {
            StartImageLoad(image, url, _actions.LoadLocalImageAsync, _imageLoads.Token);
        }
        return panel;
    }

    private Control CreateYoutubeCard(AstNode node)
    {
        var videoId = node.AttributeString("video_id", "videoId", "id") ?? node.Text ?? string.Empty;
        var start = node.AttributeInt("start") ?? 0;
        var url = $"https://www.youtube.com/watch?v={Uri.EscapeDataString(videoId)}" + (start > 0 ? $"&t={start}" : string.Empty);
        var card = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 2,
            Padding = new Padding(10),
            BackColor = Color.FromArgb(36, 36, 36),
            Margin = new Padding(0, 4, 0, 12),
        };
        card.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        card.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        var thumbnail = new PictureBox
        {
            Size = new Size(160, 90),
            SizeMode = PictureBoxSizeMode.Zoom,
            BackColor = Color.FromArgb(65, 65, 65),
            AccessibleName = "YouTube thumbnail",
        };
        card.Controls.Add(thumbnail, 0, 0);
        var play = new LinkLabel
        {
            AutoSize = true,
            Text = $"▶ YouTube\n{videoId}\n기본 브라우저에서 열기",
            LinkColor = Color.White,
            ActiveLinkColor = Color.FromArgb(255, 176, 132),
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, FontStyle.Bold),
            Margin = new Padding(12, 8, 0, 0),
        };
        play.LinkClicked += async (_, _) => await _actions.OpenExternalAsync(url);
        card.Controls.Add(play, 1, 0);
        if (Uri.TryCreate($"https://i.ytimg.com/vi/{Uri.EscapeDataString(videoId)}/hqdefault.jpg", UriKind.Absolute, out var thumbnailUri))
        {
            StartImageLoad(thumbnail, thumbnailUri, _actions.LoadRemoteImageAsync, _imageLoads.Token);
        }

        return card;
    }

    private Control CreateTagDocumentList(AstNode node)
    {
        var flow = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = true,
            Margin = new Padding(0, 3, 0, 10),
        };
        // The AST serializer deliberately supplies rich link children. Prefer
        // them: attrs.documents is metadata rather than a nested AST list.
        var documents = node.Children.Count > 0
            ? node.Children
            : node.AttributeNodeList("documents", "items");

        foreach (var item in documents)
        {
            var documentUrl = item.AttributeString("url", "href");
            var slug = documentUrl?.StartsWith("/doc/", StringComparison.OrdinalIgnoreCase) == true
                ? GetInternalDocumentSlug(documentUrl)
                : GetInternalDocumentSlug(item.AttributeString("slug") ?? item.Text ?? string.Empty);
            var title = item.AttributeString("title", "label") ?? item.Text ?? slug;
            if (string.IsNullOrWhiteSpace(slug))
            {
                continue;
            }
            var link = new LinkLabel { AutoSize = true, Text = title, Margin = new Padding(0, 0, 4, 0) };
            link.LinkClicked += async (_, _) => await _actions.OpenDocumentAsync(slug);
            flow.Controls.Add(link);
        }

        if (flow.Controls.Count == 0)
        {
            // Be tolerant of older servers that supplied plain document DTOs
            // only in attrs.documents instead of serializer-created links.
            foreach (var document in node.AttributeObjectList("documents", "items"))
            {
                var slug = JsonValue.String(document, "slug", "target") ?? string.Empty;
                var title = JsonValue.String(document, "title", "label") ?? slug;
                if (string.IsNullOrWhiteSpace(slug))
                {
                    continue;
                }

                var link = new LinkLabel { AutoSize = true, Text = title, Margin = new Padding(0, 0, 4, 0) };
                link.LinkClicked += async (_, _) => await _actions.OpenDocumentAsync(slug);
                flow.Controls.Add(link);
            }
        }

        if (flow.Controls.Count == 0)
        {
            flow.Controls.Add(new Label { AutoSize = true, Text = "해당 태그 문서가 없습니다.", ForeColor = Color.DimGray });
        }

        return flow;
    }

    private Control CreateRawEmbed(AstNode node)
    {
        var kind = NormalizeType(node.AttributeString("kind", "embed_type", "type") ?? string.Empty);
        return kind switch
        {
            "image" => CreateImageBlock(node),
            "youtube" => CreateYoutubeCard(node),
            "tag" or "tag_embed" => CreateTagDocumentList(node),
            _ when node.Children.Count > 0 => CreateNestedBlocks(node),
            _ => CreateSafeRawText(node),
        };
    }

    private Control CreateSafeRawText(AstNode node)
    {
        // Arbitrary raw HTML is intentionally not interpreted. PersonalWiki's official
        // syntax has dedicated AST nodes; unknown HTML stays visible and harmless.
        var raw = node.Text ?? InlinePlainText(node.Children, "");
        var text = raw.Replace("<br>", Environment.NewLine, StringComparison.OrdinalIgnoreCase)
            .Replace("<br/>", Environment.NewLine, StringComparison.OrdinalIgnoreCase)
            .Replace("<br />", Environment.NewLine, StringComparison.OrdinalIgnoreCase);
        return new Label
        {
            AutoSize = true,
            Text = text,
            ForeColor = Color.DimGray,
            Font = _documentFont,
            MaximumSize = new Size(900, 0),
        };
    }

    private Control CreateFootnotes(AstNode node)
    {
        var panel = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 1,
            Dock = DockStyle.Top,
            Padding = new Padding(0, 12, 0, 0),
            Margin = new Padding(0, 8, 0, 0),
        };
        panel.Controls.Add(new Label
        {
            AutoSize = true,
            Text = "각주",
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, FontStyle.Bold),
            Margin = new Padding(0, 0, 0, 4),
        }, 0, 0);
        var row = 1;
        foreach (var item in node.Children)
        {
            var definition = CreateFootnoteItem(item);
            panel.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            panel.Controls.Add(definition, 0, row++);
        }

        return panel;
    }

    private Control CreateFootnoteItem(AstNode node)
    {
        var index = node.AttributeString("index", "key", "id") ?? node.Text ?? "?";
        var outer = new Panel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Dock = DockStyle.Top,
            Padding = new Padding(0, 2, 0, 2),
        };
        var number = new LinkLabel
        {
            AutoSize = true,
            Text = $"[{index}] ",
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, FontStyle.Bold),
            Dock = DockStyle.Left,
        };
        number.LinkClicked += (_, _) =>
        {
            if (_anchors.TryGetValue($"footnote-ref-{index}", out var reference))
            {
                _scrollHost.ScrollControlIntoView(reference);
            }
        };
        var body = CreateBlockTable(node.Children.Count > 0
            ? node.Children
            : [new AstNode("paragraph", node.Text, null, [])]);
        outer.Controls.Add(body);
        outer.Controls.Add(number);
        _anchors[$"footnote-{index}"] = outer;
        return outer;
    }

    private FlowLayoutPanel CreateInlineFlow(IReadOnlyList<AstNode> nodes)
    {
        var flow = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = true,
            Margin = new Padding(0),
            Padding = new Padding(0),
        };
        AddInlineNodes(flow, nodes, FontStyle.Regular, Color.FromArgb(58, 58, 58), null);

        return flow;
    }

    private void AddInline(FlowLayoutPanel host, AstNode node, FontStyle inheritedStyle, Color inheritedColor, Color? background)
    {
        var type = NormalizeType(node.Type);
        switch (type)
        {
            case "text":
                AddText(host, node.Text ?? string.Empty, inheritedStyle, inheritedColor, background);
                return;
            case "softbreak":
            case "linebreak":
            case "hardbreak":
                if (host.Controls.Count == 0)
                {
                    var spacer = new Label { AutoSize = false, Size = new Size(0, _documentFont.Height) };
                    host.Controls.Add(spacer);
                    host.SetFlowBreak(spacer, true);
                }
                else
                {
                    host.SetFlowBreak(host.Controls[^1], true);
                }
                return;
            case "emphasis":
            case "italic":
                AddInlineChildren(host, node, inheritedStyle | FontStyle.Italic, inheritedColor, background);
                return;
            case "strong":
            case "bold":
                AddInlineChildren(host, node, inheritedStyle | FontStyle.Bold, inheritedColor, background);
                return;
            case "strikethrough":
            case "strike":
                AddInlineChildren(host, node, inheritedStyle | FontStyle.Strikeout, inheritedColor, background);
                return;
            case "highlight":
            case "mark":
                AddInlineChildren(host, node, inheritedStyle, inheritedColor, Color.FromArgb(255, 234, 102));
                return;
            case "inline_code":
            case "codespan":
            case "code":
                AddText(host, InlinePlainText(node.Children, node.Text), FontStyle.Regular, Color.FromArgb(26, 26, 26), Color.FromArgb(245, 240, 224), "Cascadia Code");
                return;
            case "kbd":
                AddKeyboardKey(host, InlinePlainText(node.Children, node.Text));
                return;
            case "link":
            case "wiki_link":
                AddLink(host, node, inheritedStyle);
                return;
            case "image":
            case "image_embed":
                host.Controls.Add(CreateImageBlock(node));
                return;
            case "youtube":
            case "youtube_embed":
                host.Controls.Add(CreateYoutubeCard(node));
                return;
            case "tag_embed":
            case "tag_document_list":
                host.Controls.Add(CreateTagDocumentList(node));
                return;
            case "spoiler":
                AddSpoiler(host, node);
                return;
            case "template_inline":
                AddInlineChildren(host, node, inheritedStyle, inheritedColor, background);
                return;
            case "template":
                if (node.AttributeBool("folded") == true)
                {
                    host.Controls.Add(CreateFoldedTemplate(node));
                }
                else
                {
                    AddInlineChildren(host, node, inheritedStyle, inheritedColor, background);
                }
                return;
            case "folded_template_inline":
            case "folded_template_block":
                host.Controls.Add(CreateFoldedTemplate(node));
                return;
            case "raw_embed":
                host.Controls.Add(CreateRawEmbed(node));
                return;
            case "html":
            case "inline_html":
                AddText(host, node.Text ?? string.Empty, inheritedStyle, Color.DimGray, background);
                return;
            case "footnote_ref":
                AddFootnoteReference(host, node, inheritedStyle);
                return;
            default:
                if (node.Children.Count > 0)
                {
                    AddInlineChildren(host, node, inheritedStyle, inheritedColor, background);
                }
                else
                {
                    AddText(host, node.Text ?? string.Empty, inheritedStyle, inheritedColor, background);
                }

                return;
        }
    }

    private void AddInlineChildren(FlowLayoutPanel host, AstNode node, FontStyle style, Color color, Color? background)
    {
        if (node.Children.Count == 0)
        {
            AddText(host, node.Text ?? string.Empty, style, color, background);
            return;
        }

        AddInlineNodes(host, node.Children, style, color, background);
    }

    /// <summary>
    /// Mistune can split one visible run into several adjacent text AST nodes.
    /// One WinForms Label per fragment is expensive for long documents, so
    /// merge only equivalent contiguous text while leaving styled/link/action
    /// nodes as independently interactive controls.
    /// </summary>
    private void AddInlineNodes(
        FlowLayoutPanel host,
        IReadOnlyList<AstNode> nodes,
        FontStyle style,
        Color color,
        Color? background)
    {
        string? firstText = null;
        StringBuilder? textBuffer = null;

        void FlushText()
        {
            if (textBuffer is not null)
            {
                AddText(host, textBuffer.ToString(), style, color, background);
            }
            else if (firstText is not null)
            {
                AddText(host, firstText, style, color, background);
            }

            firstText = null;
            textBuffer = null;
        }

        foreach (var child in nodes)
        {
            if (child.Type.Equals("text", StringComparison.OrdinalIgnoreCase))
            {
                var value = child.Text ?? string.Empty;
                if (firstText is null)
                {
                    firstText = value;
                }
                else if (textBuffer is null)
                {
                    textBuffer = new StringBuilder(firstText.Length + value.Length);
                    textBuffer.Append(firstText);
                    textBuffer.Append(value);
                }
                else
                {
                    textBuffer.Append(value);
                }

                continue;
            }

            FlushText();
            AddInline(host, child, style, color, background);
        }

        FlushText();
    }

    private void AddLink(FlowLayoutPanel host, AstNode node, FontStyle style)
    {
        var url = node.AttributeString("url", "href", "target") ?? string.Empty;
        var text = InlinePlainText(node.Children, node.Text);
        var link = new LinkLabel
        {
            AutoSize = true,
            Text = string.IsNullOrWhiteSpace(text) ? url : text,
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, style | FontStyle.Underline),
            LinkColor = Color.FromArgb(9, 105, 218),
            ActiveLinkColor = Color.FromArgb(5, 80, 170),
            Margin = new Padding(0),
        };
        link.LinkClicked += async (_, _) =>
        {
            if (url.StartsWith('#'))
            {
                ScrollToLocalAnchor(url);
            }
            else if (url.StartsWith("/doc/", StringComparison.OrdinalIgnoreCase))
            {
                var slug = GetInternalDocumentSlug(url);
                if (slug is not null)
                {
                    await _actions.OpenDocumentAsync(slug);
                }
            }
            else if (url.Equals("/new", StringComparison.OrdinalIgnoreCase)
                     || url.StartsWith("/new?", StringComparison.OrdinalIgnoreCase))
            {
                var title = GetQueryValue(url, "title") ?? text;
                if (!title.Any(char.IsControl))
                {
                    await _actions.CreateDocumentAsync(title);
                }
            }
            else if (url.StartsWith("/file/", StringComparison.OrdinalIgnoreCase))
            {
                await _actions.DownloadFileAsync(url.TrimStart('/'));
            }
            else if (RendererActions.TryGetAllowedExternalUri(url, out var external))
            {
                await _actions.OpenExternalAsync(external.AbsoluteUri);
            }
        };
        host.Controls.Add(link);
    }

    private void ScrollToLocalAnchor(string url)
    {
        var anchor = DecodeUriComponent(url[1..]);
        if (anchor is not null && anchor.Length > 0 && _anchors.TryGetValue(anchor, out var target))
        {
            _scrollHost.ScrollControlIntoView(target);
        }
    }

    private void AddFootnoteReference(FlowLayoutPanel host, AstNode node, FontStyle style)
    {
        var index = node.AttributeString("index", "key", "id") ?? node.Text ?? "?";
        var reference = new LinkLabel
        {
            AutoSize = true,
            Text = $"[{index}]",
            Font = FontCache.Get(_documentFont.FontFamily.Name, Math.Max(7, _documentFont.Size - 1), style | FontStyle.Underline),
            LinkColor = Color.FromArgb(9, 105, 218),
            Margin = new Padding(1, 0, 1, 0),
        };
        reference.LinkClicked += (_, _) =>
        {
            if (_anchors.TryGetValue($"footnote-{index}", out var definition))
            {
                _scrollHost.ScrollControlIntoView(definition);
            }
        };
        _anchors[$"footnote-ref-{index}"] = reference;
        host.Controls.Add(reference);
    }

    private void AddSpoiler(FlowLayoutPanel host, AstNode node)
    {
        var content = InlinePlainText(node.Children, node.Text);
        var spoiler = new LinkLabel
        {
            AutoSize = true,
            Text = "[스포일러: 클릭하여 표시]",
            LinkColor = Color.FromArgb(80, 80, 80),
            BackColor = Color.FromArgb(38, 38, 38),
            ForeColor = Color.White,
            Margin = new Padding(0),
        };
        spoiler.LinkClicked += (_, _) =>
        {
            spoiler.Text = content;
            spoiler.BackColor = Color.FromArgb(240, 240, 240);
            spoiler.ForeColor = Color.FromArgb(26, 26, 26);
        };
        host.Controls.Add(spoiler);
    }

    private void AddText(FlowLayoutPanel host, string text, FontStyle style, Color color, Color? background, string? fontFamily = null)
    {
        if (string.IsNullOrEmpty(text))
        {
            return;
        }

        var label = new Label
        {
            AutoSize = true,
            Text = text,
            Font = FontCache.Get(fontFamily ?? _documentFont.FontFamily.Name, _documentFont.Size, style),
            ForeColor = color,
            BackColor = background ?? Color.Transparent,
            Margin = new Padding(0),
            Padding = background is null ? Padding.Empty : new Padding(2, 1, 2, 1),
        };
        host.Controls.Add(label);
    }

    private void AddKeyboardKey(FlowLayoutPanel host, string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return;
        }

        host.Controls.Add(new Label
        {
            AutoSize = true,
            Text = text,
            Font = FontCache.Get(_documentFont.FontFamily.Name, Math.Max(8, _documentFont.Size - 1), FontStyle.Bold),
            ForeColor = Color.FromArgb(45, 45, 45),
            BackColor = Color.FromArgb(239, 236, 226),
            BorderStyle = BorderStyle.FixedSingle,
            Padding = new Padding(4, 1, 4, 1),
            Margin = new Padding(1, 0, 1, 0),
            AccessibleName = "keyboard key",
        });
    }

    private TableLayoutPanel CreateBlockTable(IReadOnlyList<AstNode> nodes)
    {
        var panel = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 1,
            Dock = DockStyle.Top,
            Margin = new Padding(0),
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        foreach (var child in nodes)
        {
            AddBlock(panel, child);
        }

        return panel;
    }

    private void StartImageLoad(
        PictureBox target,
        string source,
        Func<string, CancellationToken, Task<Image?>> loader,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(source) || Volatile.Read(ref _rendererDisposed) != 0)
        {
            return;
        }

        Interlocked.Increment(ref _pendingImageLoads);
        _ = LoadImageAsync(target, source, loader, cancellationToken);
    }

    private void StartImageLoad(
        PictureBox target,
        Uri source,
        Func<Uri, CancellationToken, Task<Image?>> loader,
        CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _rendererDisposed) != 0)
        {
            return;
        }

        Interlocked.Increment(ref _pendingImageLoads);
        _ = LoadImageAsync(target, source, loader, cancellationToken);
    }

    private async Task LoadImageAsync(
        PictureBox target,
        string source,
        Func<string, CancellationToken, Task<Image?>> loader,
        CancellationToken cancellationToken)
    {
        Image? image = null;
        var slotAcquired = false;
        try
        {
            await _imageLoadSlots.WaitAsync(cancellationToken);
            slotAcquired = true;
            cancellationToken.ThrowIfCancellationRequested();
            image = await loader(source, cancellationToken);
            if (!cancellationToken.IsCancellationRequested && !target.IsDisposed && image is not null)
            {
                target.Image?.Dispose();
                target.Image = image;
                image = null;
            }
        }
        catch (Exception)
        {
            // A missing image must not prevent the rest of a document from rendering.
        }
        finally
        {
            image?.Dispose();
            if (slotAcquired)
            {
                _imageLoadSlots.Release();
            }

            CompleteImageLoad();
        }
    }

    private async Task LoadImageAsync(
        PictureBox target,
        Uri source,
        Func<Uri, CancellationToken, Task<Image?>> loader,
        CancellationToken cancellationToken)
    {
        Image? image = null;
        var slotAcquired = false;
        try
        {
            await _imageLoadSlots.WaitAsync(cancellationToken);
            slotAcquired = true;
            cancellationToken.ThrowIfCancellationRequested();
            image = await loader(source, cancellationToken);
            if (!cancellationToken.IsCancellationRequested && !target.IsDisposed && image is not null)
            {
                target.Image?.Dispose();
                target.Image = image;
                image = null;
            }
        }
        catch (Exception)
        {
            // Thumbnail retrieval is optional; the card remains functional without it.
        }
        finally
        {
            image?.Dispose();
            if (slotAcquired)
            {
                _imageLoadSlots.Release();
            }

            CompleteImageLoad();
        }
    }

    private static string InlinePlainText(IReadOnlyList<AstNode> nodes, string? fallback)
    {
        if (nodes.Count == 0)
        {
            return fallback ?? string.Empty;
        }

        return string.Concat(nodes.Select(node => InlinePlainText(node.Children, node.Text)));
    }

    private static string? GetQueryValue(string url, string name)
    {
        var question = url.IndexOf('?');
        if (question < 0 || question >= url.Length - 1)
        {
            return null;
        }

        foreach (var pair in url[(question + 1)..].Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = pair.Split('=', 2);
            if (parts.Length != 2)
            {
                continue;
            }

            var key = DecodeUriComponent(parts[0]);
            if (key is not null && string.Equals(key, name, StringComparison.OrdinalIgnoreCase))
            {
                return DecodeUriComponent(parts[1].Replace('+', ' '));
            }
        }

        return null;
    }

    /// <summary>
    /// Decodes one URI component used by an AST-provided local action. A
    /// malformed escape or decoded control character must never escape into a
    /// document navigation, editor title, or WinForms control action.
    /// </summary>
    private static string? DecodeUriComponent(string encoded)
    {
        if (string.IsNullOrEmpty(encoded) || HasMalformedPercentEscape(encoded))
        {
            return null;
        }

        try
        {
            var decoded = Uri.UnescapeDataString(encoded);
            return decoded.Any(char.IsControl) ? null : decoded;
        }
        catch (UriFormatException)
        {
            return null;
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    private static string? GetInternalDocumentSlug(string source)
    {
        var encoded = source.StartsWith("/doc/", StringComparison.OrdinalIgnoreCase)
            ? source[5..]
            : source;
        var slug = DecodeUriComponent(encoded);
        return string.IsNullOrWhiteSpace(slug)
               || slug is "." or ".."
               || slug.IndexOfAny(['/', '\\']) >= 0
            ? null
            : slug;
    }

    private static bool HasMalformedPercentEscape(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            if (value[index] != '%')
            {
                continue;
            }

            if (index + 2 >= value.Length
                || !Uri.IsHexDigit(value[index + 1])
                || !Uri.IsHexDigit(value[index + 2]))
            {
                return true;
            }

            index += 2;
        }

        return false;
    }

    private static string NormalizeType(string value) => (value ?? string.Empty)
        .Trim()
        .Replace('-', '_')
        .Replace(' ', '_')
        .ToLowerInvariant();

    private void ResetImageLoads()
    {
        var previous = _imageLoads;
        _imageLoads = new CancellationTokenSource();
        previous.Cancel();
        previous.Dispose();
    }

    private void CancelImageLoads()
    {
        if (!_imageLoads.IsCancellationRequested)
        {
            _imageLoads.Cancel();
        }
    }

    private void CompleteImageLoad()
    {
        if (Interlocked.Decrement(ref _pendingImageLoads) == 0)
        {
            DisposeImageLoadSlotsWhenIdle();
        }
    }

    private void DisposeImageLoadSlotsWhenIdle()
    {
        if (Volatile.Read(ref _rendererDisposed) == 0
            || Volatile.Read(ref _pendingImageLoads) != 0
            || Interlocked.Exchange(ref _imageLoadSlotsDisposed, 1) != 0)
        {
            return;
        }

        _imageLoadSlots.Dispose();
    }

    private static void DisposeChildControls(Control parent)
    {
        foreach (Control child in parent.Controls.Cast<Control>().ToArray())
        {
            DisposeControlTree(child);
        }
    }

    private static void DisposeControlTree(Control control)
    {
        DisposePictureBoxImages(control);
        // Control.Dispose recursively disposes children. Walking the tree to
        // call Dispose on every descendant first caused redundant disposal and
        // teardown work every time a large document was replaced.
        control.Dispose();
    }

    private static void DisposePictureBoxImages(Control control)
    {
        foreach (Control child in control.Controls.Cast<Control>().ToArray())
        {
            DisposePictureBoxImages(child);
        }

        if (control is PictureBox picture)
        {
            var image = picture.Image;
            picture.Image = null;
            image?.Dispose();
        }
    }
}

internal static class FontCache
{
    private static readonly Dictionary<(string Family, float Size, FontStyle Style), Font> Fonts = new();

    public static Font Get(string family, float size, FontStyle style)
    {
        var key = (Family: family, Size: Math.Max(6, size), Style: style);
        if (Fonts.TryGetValue(key, out var existing))
        {
            return existing;
        }

        try
        {
            return Fonts[key] = new Font(key.Family, key.Size, key.Style, GraphicsUnit.Point);
        }
        catch (ArgumentException)
        {
            return Fonts[key] = new Font("Segoe UI", key.Size, key.Style, GraphicsUnit.Point);
        }
    }

    public static void DisposeCachedFonts()
    {
        foreach (var font in Fonts.Values)
        {
            font.Dispose();
        }

        Fonts.Clear();
    }
}
