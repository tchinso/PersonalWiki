using System.Diagnostics;
using System.Drawing.Text;
using System.Net;

namespace PersonalWikiClient;

/// <summary>
/// A deliberately small local reader/editor.  Its only server interaction is
/// the localhost JSON client API; it neither reads the PersonalWiki database
/// nor embeds any web rendering control.
/// </summary>
internal sealed class MainForm : Form
{
    private const int MaxRemoteImageBytes = 8 * 1024 * 1024;
    // Local images are trusted more than remote thumbnails, but an accidental
    // 100 MB paste in img should not make this deliberately light reader
    // allocate it wholesale merely to paint a 960 px PictureBox.
    private const int MaxLocalImageBytes = 32 * 1024 * 1024;
    private static readonly StringComparer DocumentTitleComparer = StringComparer.CurrentCultureIgnoreCase;

    private readonly SplitContainer _layout = new()
    {
        Dock = DockStyle.Fill,
        FixedPanel = FixedPanel.Panel1,
        IsSplitterFixed = false,
        SplitterWidth = 5,
    };
    private readonly ListBox _documentList = new()
    {
        Dock = DockStyle.Fill,
        BorderStyle = BorderStyle.None,
        IntegralHeight = false,
    };
    private readonly TextBox _searchBox = new()
    {
        Dock = DockStyle.Top,
        PlaceholderText = "문서 검색",
        Margin = new Padding(12, 8, 12, 8),
    };
    private readonly Label _endpointLabel = new()
    {
        AutoSize = true,
        Dock = DockStyle.Bottom,
        Padding = new Padding(12, 7, 12, 7),
        ForeColor = Color.DimGray,
    };
    private readonly Label _statusLabel = new()
    {
        AutoSize = true,
        Dock = DockStyle.Bottom,
        Padding = new Padding(12, 4, 12, 3),
        ForeColor = Color.DimGray,
    };
    private readonly Panel _workspace = new()
    {
        Dock = DockStyle.Fill,
        BackColor = Color.FromArgb(255, 250, 240),
    };
    private readonly Label _titleLabel = new()
    {
        AutoSize = true,
        Dock = DockStyle.Top,
        Padding = new Padding(18, 7, 8, 0),
        Font = new Font("Segoe UI", 17, FontStyle.Bold),
    };
    private readonly Label _metadataLabel = new()
    {
        AutoSize = true,
        Dock = DockStyle.Top,
        Padding = new Padding(19, 0, 8, 0),
        ForeColor = Color.DimGray,
    };
    private readonly FlowLayoutPanel _tagBar = new()
    {
        AutoSize = true,
        AutoSizeMode = AutoSizeMode.GrowAndShrink,
        Dock = DockStyle.Top,
        Padding = new Padding(18, 2, 8, 2),
        WrapContents = false,
        FlowDirection = FlowDirection.LeftToRight,
    };
    private readonly Button _editButton = new()
    {
        Text = "편집",
        AutoSize = true,
    };
    private readonly Button _deleteButton = new()
    {
        Text = "삭제",
        AutoSize = true,
    };
    private readonly System.Windows.Forms.Timer _searchTimer = new()
    {
        Interval = 300,
    };
    private readonly CancellationTokenSource _lifetime = new();
    private readonly ClipboardImageAttachments _attachments = new();
    private readonly HttpClient _remoteImageHttp = new(new HttpClientHandler
    {
        AllowAutoRedirect = false,
        UseCookies = false,
    })
    {
        Timeout = TimeSpan.FromSeconds(10),
    };

    private ClientSettings _settings;
    private ClientApi _api;
    private Font _documentFont;
    private CancellationTokenSource? _operation;
    private List<DocumentSummary> _documents = [];
    private WikiDocument? _activeDocument;
    private string? _activeSlug;
    private NativeMarkdownRenderer? _documentRenderer;
    private DocumentScrollRestore? _documentScrollPositionBeforeEditor;
    private bool _suppressSelection;
    private bool _isEditing;
    private readonly List<ClientImageAttachment> _attachmentsForCurrentEdit = [];
    private TextBox? _editTitle;
    private TextBox? _editTags;
    private TextBox? _editContent;

    public MainForm()
    {
        _settings = ClientSettings.Load();
        _api = new ClientApi(_settings.ServerPort);
        _documentFont = CreateClientFont(_settings.FontFamily, 10.5f);

        Text = "PersonalWikiClient";
        StartPosition = FormStartPosition.CenterScreen;
        MinimumSize = new Size(840, 560);
        Size = new Size(1200, 780);
        Font = _documentFont;
        BackColor = Color.FromArgb(255, 250, 240);

        BuildNavigation();
        BuildWorkspace();
        Controls.Add(_layout);
        // SplitterDistance is clamped against a SplitContainer's tiny default
        // width when set in its field initializer. Apply it only after docking
        // it into the already-sized form so the navigation controls are visible.
        _layout.Panel1MinSize = 320;
        _layout.SplitterDistance = 340;

        Shown += async (_, _) => await ReloadDocumentsAsync();
        FormClosed += (_, _) => DisposeResources();
    }

    private void BuildNavigation()
    {
        var navigation = _layout.Panel1;
        navigation.BackColor = Color.FromArgb(247, 243, 232);
        navigation.Padding = new Padding(0, 8, 0, 0);

        var heading = new Label
        {
            AutoSize = true,
            Dock = DockStyle.Top,
            Text = "PersonalWikiClient",
            Padding = new Padding(12, 8, 8, 0),
            Font = new Font(_documentFont.FontFamily, 13, FontStyle.Bold),
        };
        navigation.Controls.Add(_documentList);
        navigation.Controls.Add(_searchBox);
        navigation.Controls.Add(CreateNavigationButtons());
        navigation.Controls.Add(heading);
        navigation.Controls.Add(_statusLabel);
        navigation.Controls.Add(_endpointLabel);

        _documentList.DisplayMember = nameof(DocumentListItem.Text);
        _documentList.SelectedIndexChanged += async (_, _) =>
        {
            if (!_suppressSelection && _documentList.SelectedItem is DocumentListItem item)
            {
                await LoadDocumentAsync(item.Document.Slug);
            }
        };
        _searchBox.TextChanged += (_, _) =>
        {
            _searchTimer.Stop();
            _searchTimer.Start();
        };
        _searchTimer.Tick += async (_, _) =>
        {
            _searchTimer.Stop();
            await SearchAsync(_searchBox.Text);
        };
        UpdateEndpointLabel();
    }

    private Control CreateNavigationButtons()
    {
        var buttons = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Dock = DockStyle.Top,
            Padding = new Padding(12, 2, 12, 4),
            WrapContents = true,
        };
        var create = new Button { Text = "새 문서", AutoSize = true };
        var refresh = new Button { Text = "새로 고침", AutoSize = true };
        var settings = new Button { Text = "연결·글꼴", AutoSize = true };
        create.Click += (_, _) => BeginNewDocument();
        refresh.Click += async (_, _) => await ReloadDocumentsAsync();
        settings.Click += async (_, _) => await EditSettingsAsync();
        buttons.Controls.AddRange([create, refresh, settings]);
        return buttons;
    }

    private void BuildWorkspace()
    {
        var right = _layout.Panel2;
        right.BackColor = Color.FromArgb(255, 250, 240);
        var actionBar = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Dock = DockStyle.Top,
            Padding = new Padding(18, 3, 8, 4),
            WrapContents = false,
        };
        _editButton.Click += (_, _) => BeginEditActiveDocument();
        _deleteButton.Click += async (_, _) => await DeleteActiveDocumentAsync();
        actionBar.Controls.AddRange([_editButton, _deleteButton]);
        right.Controls.Add(_workspace);
        right.Controls.Add(_tagBar);
        right.Controls.Add(_metadataLabel);
        right.Controls.Add(_titleLabel);
        right.Controls.Add(actionBar);
        ShowWelcome();
    }

    private async Task ReloadDocumentsAsync()
    {
        var token = BeginOperation();
        SetStatus("문서 목록을 불러오는 중…");
        try
        {
            _documents = (await _api.GetDocumentsAsync(token)).ToList();
            // List.Sort keeps the existing alphabetical navigation behavior
            // without the extra ordered-enumerable buffers used by OrderBy.
            _documents.Sort((left, right) => DocumentTitleComparer.Compare(left.Title, right.Title));
            if (token.IsCancellationRequested || IsDisposed)
            {
                return;
            }

            PopulateDocumentList(_documents);
            SetStatus($"문서 {_documents.Count}개");
            if (_activeDocument is null && !_isEditing)
            {
                ShowWelcome();
            }
        }
        catch (OperationCanceledException)
        {
            // A newer navigation operation replaced this one.
        }
        catch (Exception error)
        {
            ReportConnectionFailure(error);
        }
    }

    private async Task SearchAsync(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            PopulateDocumentList(_documents);
            return;
        }

        var token = BeginOperation();
        SetStatus("검색 중…");
        try
        {
            var results = await _api.SearchAsync(query.Trim(), token);
            if (!token.IsCancellationRequested && !IsDisposed)
            {
                PopulateDocumentList(results);
                SetStatus($"검색 결과 {results.Count}개");
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception error)
        {
            ReportConnectionFailure(error);
        }
    }

    private async Task LoadDocumentAsync(string slug)
    {
        if (string.IsNullOrWhiteSpace(slug))
        {
            return;
        }

        if (_isEditing)
        {
            if (!ConfirmDiscardChanges())
            {
                SelectDocumentInList(_activeSlug);
                return;
            }

            // This is navigation away from an abandoned draft, not a return
            // from Save/Cancel. Do not apply its old document viewport to the
            // newly selected document.
            _documentScrollPositionBeforeEditor = null;
        }

        var token = BeginOperation();
        SetStatus("문서를 불러오는 중…");
        try
        {
            var payload = await _api.GetDocumentAsync(slug, token);
            if (token.IsCancellationRequested || IsDisposed)
            {
                return;
            }

            _activeDocument = payload.Document;
            _activeSlug = payload.Document.Slug;
            _isEditing = false;
            ShowDocument(payload);
            SelectDocumentInList(_activeSlug);
            SetStatus("준비됨");
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception error)
        {
            ReportConnectionFailure(error);
        }
    }

    private void ShowDocument(DocumentPayload payload)
    {
        SetHeader(payload.Document.Title, payload.Document.Tags, payload.Document.UpdatedAt, true);
        var renderer = new NativeMarkdownRenderer { Dock = DockStyle.Fill };
        renderer.Render(payload.Ast, new RendererActions
        {
            OpenDocumentAsync = LoadDocumentAsync,
            CreateDocumentAsync = title =>
            {
                BeginNewDocument(title);
                return Task.CompletedTask;
            },
            OpenExternalAsync = OpenExternalAsync,
            DownloadFileAsync = DownloadFileAsync,
            LoadLocalImageAsync = LoadLocalImageAsync,
            LoadRemoteImageAsync = LoadRemoteImageAsync,
        }, _documentFont);
        _workspace.SuspendLayout();
        try
        {
            ClearWorkspace();
            _documentRenderer = renderer;
            _workspace.Controls.Add(renderer);

            if (payload.Backlinks.Count > 0)
            {
                var backlinks = CreateBacklinks(payload.Backlinks);
                _workspace.Controls.Add(backlinks);
                backlinks.BringToFront();
            }
        }
        finally
        {
            _workspace.ResumeLayout(true);
        }

        if (_documentScrollPositionBeforeEditor is DocumentScrollRestore savedScrollPosition)
        {
            // Layout the fresh renderer before applying the old offset. This
            // restores the reader viewport before the UI paints instead of
            // visibly jumping from the top after Save or Cancel.
            _documentScrollPositionBeforeEditor = null;
            if (string.Equals(savedScrollPosition.Slug, payload.Document.Slug, StringComparison.OrdinalIgnoreCase))
            {
                _workspace.PerformLayout();
                renderer.PerformLayout();
                renderer.RestoreScrollPosition(savedScrollPosition.Position);
            }
        }
    }

    private Control CreateBacklinks(IReadOnlyList<DocumentSummary> backlinks)
    {
        var panel = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Dock = DockStyle.Bottom,
            Padding = new Padding(18, 6, 18, 9),
            BackColor = Color.FromArgb(247, 243, 232),
            WrapContents = true,
        };
        panel.Controls.Add(new Label
        {
            AutoSize = true,
            Text = "연결된 문서: ",
            Font = FontCache.Get(_documentFont.FontFamily.Name, _documentFont.Size, FontStyle.Bold),
        });
        foreach (var backlink in backlinks)
        {
            var link = new LinkLabel { AutoSize = true, Text = backlink.Title, Margin = new Padding(0, 0, 7, 0) };
            link.LinkClicked += async (_, _) => await LoadDocumentAsync(backlink.Slug);
            panel.Controls.Add(link);
        }

        return panel;
    }

    private void BeginNewDocument(string? prefilledTitle = null)
    {
        if (_isEditing && !ConfirmDiscardChanges())
        {
            return;
        }

        _activeDocument = null;
        _activeSlug = null;
        _documentScrollPositionBeforeEditor = null;
        _isEditing = true;
        _attachmentsForCurrentEdit.Clear();
        ShowEditor(new WikiDocument(prefilledTitle ?? string.Empty, string.Empty, [], string.Empty, null, null));
    }

    private void BeginEditActiveDocument()
    {
        if (_activeDocument is null)
        {
            BeginNewDocument();
            return;
        }

        _documentScrollPositionBeforeEditor = _documentRenderer is { IsDisposed: false } renderer
            && !string.IsNullOrWhiteSpace(_activeSlug)
            ? new DocumentScrollRestore(_activeSlug, renderer.GetScrollPosition())
            : null;
        _isEditing = true;
        _attachmentsForCurrentEdit.Clear();
        ShowEditor(_activeDocument);
    }

    private void ShowEditor(WikiDocument document)
    {
        SetHeader(_activeSlug is null ? "새 문서" : "문서 편집", [], null, false);
        var editor = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 4,
            Padding = new Padding(20, 14, 20, 18),
            BackColor = Color.FromArgb(255, 250, 240),
        };
        editor.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        editor.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        editor.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        editor.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        editor.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        _editTitle = new TextBox { Dock = DockStyle.Top, Text = document.Title, Margin = new Padding(0, 0, 0, 8) };
        _editTags = new TextBox { Dock = DockStyle.Top, Text = string.Join(", ", document.Tags), Margin = new Padding(0, 0, 0, 8) };
        _editContent = new TextBox
        {
            Dock = DockStyle.Fill,
            Multiline = true,
            AcceptsTab = true,
            ScrollBars = ScrollBars.Both,
            WordWrap = false,
            Text = document.Content,
            Font = FontCache.Get("Cascadia Code", Math.Max(9, _documentFont.Size), FontStyle.Regular),
            AllowDrop = true,
        };
        _editContent.KeyDown += EditorContentKeyDown;
        _editContent.DragEnter += EditorContentDragEnter;
        _editContent.DragDrop += EditorContentDragDrop;
        var titleRow = Field("제목", _editTitle);
        var tagRow = Field("태그 (쉼표 구분)", _editTags);
        var buttons = new FlowLayoutPanel { AutoSize = true, AutoSizeMode = AutoSizeMode.GrowAndShrink, Dock = DockStyle.Top, Padding = new Padding(0, 10, 0, 0) };
        var save = new Button { Text = "저장", AutoSize = true };
        var preview = new Button { Text = "네이티브 미리보기", AutoSize = true };
        var suggestions = new Button { Text = "태그 추천", AutoSize = true };
        var cancel = new Button { Text = "취소", AutoSize = true };
        save.Click += async (_, _) => await SaveEditorAsync();
        preview.Click += async (_, _) => await PreviewEditorAsync();
        suggestions.Click += async (_, _) => await SuggestTagsAsync();
        cancel.Click += (_, _) => CancelEdit();
        buttons.Controls.AddRange([save, preview, suggestions, cancel]);

        editor.Controls.Add(titleRow, 0, 0);
        editor.Controls.Add(tagRow, 0, 1);
        editor.Controls.Add(_editContent, 0, 2);
        editor.Controls.Add(buttons, 0, 3);
        _workspace.SuspendLayout();
        try
        {
            ClearWorkspace();
            _workspace.Controls.Add(editor);
        }
        finally
        {
            _workspace.ResumeLayout(true);
        }

        // A multiline TextBox can otherwise retain a transient native scroll
        // position while its parent view is being replaced. Set its initial
        // caret and viewport before giving focus to the title field.
        _editContent.Select(0, 0);
        _editContent.ScrollToCaret();
        _editTitle.Focus();
    }

    private static Control Field(string caption, Control value)
    {
        var panel = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Dock = DockStyle.Top,
            ColumnCount = 1,
            Margin = new Padding(0),
        };
        panel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        panel.Controls.Add(new Label { AutoSize = true, Text = caption, Margin = new Padding(0, 0, 0, 3) }, 0, 0);
        panel.Controls.Add(value, 0, 1);
        return panel;
    }

    private async Task SaveEditorAsync()
    {
        if (_editTitle is null || _editTags is null || _editContent is null)
        {
            return;
        }

        var title = _editTitle.Text.Trim();
        if (title.Length == 0)
        {
            MessageBox.Show(this, "문서 제목을 입력해 주세요.", "저장", MessageBoxButtons.OK, MessageBoxIcon.Information);
            _editTitle.Focus();
            return;
        }

        var tags = ParseTags(_editTags.Text);
        var saveAsIs = false;
        var ignoreTagWarning = false;
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var request = new SaveDocumentRequest(title, _editContent.Text, tags, saveAsIs, ignoreTagWarning);
            var token = BeginOperation();
            SetStatus("저장 중…");
            try
            {
                var result = _activeSlug is null
                    ? await _api.CreateDocumentAsync(request, token)
                    : await _api.UpdateDocumentAsync(_activeSlug, request, token);
                if (token.IsCancellationRequested || IsDisposed)
                {
                    return;
                }

                if (result.NeedsTagWarningDecision)
                {
                    var answer = MessageBox.Show(this,
                        result.Warning ?? "태그가 적습니다. 그래도 저장할까요?",
                        "태그 확인", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
                    if (answer != DialogResult.Yes)
                    {
                        SetStatus("저장을 취소했습니다.");
                        return;
                    }

                    ignoreTagWarning = true;
                    continue;
                }

                if (result.NeedsSpellcheckDecision)
                {
                    var answer = MessageBox.Show(this,
                        result.Warning ?? "맞춤법 확인 항목이 있습니다. 현재 내용 그대로 저장할까요?",
                        "맞춤법 확인", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
                    if (answer != DialogResult.Yes)
                    {
                        SetStatus("저장을 취소했습니다.");
                        return;
                    }

                    saveAsIs = true;
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(result.Error))
                {
                    MessageBox.Show(this, result.Error, "저장할 수 없음", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    SetStatus("저장 실패");
                    return;
                }

                if (result.Document is null)
                {
                    MessageBox.Show(this, result.Warning ?? "서버가 저장 결과를 반환하지 않았습니다.", "저장", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return;
                }

                _activeSlug = result.Document.Slug;
                _activeDocument = result.Document;
                if (_documentScrollPositionBeforeEditor is DocumentScrollRestore savedScrollPosition)
                {
                    // Saving a title change can change the slug. Keep the
                    // viewport associated with the saved document, rather
                    // than treating its new response as an unrelated view.
                    _documentScrollPositionBeforeEditor = savedScrollPosition with { Slug = _activeSlug };
                }
                _isEditing = false;
                var finalContent = _editContent.Text;
                var attachmentsToCopy = ClipboardImageAttachments.ReferencedInContent(finalContent, _attachmentsForCurrentEdit);
                // A confirmed server save is the only point at which the
                // client may tell the user to copy its local date folder.
                // Do it before best-effort list/render refreshes, which can
                // independently fail after the document was already saved.
                ShowAttachmentCopyInstructions(attachmentsToCopy);
                _attachmentsForCurrentEdit.Clear();
                await ReloadDocumentsAsync();
                await LoadDocumentAsync(_activeSlug);
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception error)
            {
                ReportConnectionFailure(error);
                return;
            }
        }

        MessageBox.Show(this, "저장 확인을 반복할 수 없습니다. 내용을 다시 확인해 주세요.", "저장", MessageBoxButtons.OK, MessageBoxIcon.Warning);
    }

    private async Task PreviewEditorAsync()
    {
        if (_editContent is null)
        {
            return;
        }

        var token = BeginOperation();
        SetStatus("미리보기를 만드는 중…");
        try
        {
            var rendered = await _api.RenderAsync(_editContent.Text, token);
            if (token.IsCancellationRequested || IsDisposed)
            {
                return;
            }

            using var preview = new PreviewForm(rendered.Ast, CreateRendererActions(), _documentFont);
            preview.ShowDialog(this);
            SetStatus("준비됨");
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception error)
        {
            ReportConnectionFailure(error);
        }
    }

    private async Task SuggestTagsAsync()
    {
        if (_editTitle is null || _editTags is null || _editContent is null)
        {
            return;
        }

        var token = BeginOperation();
        try
        {
            var suggestions = await _api.GetTagSuggestionsAsync(
                _editTitle.Text,
                _editContent.Text,
                _activeSlug,
                ParseTags(_editTags.Text),
                token);
            if (token.IsCancellationRequested || suggestions.Count == 0)
            {
                return;
            }

            var message = "추천 태그: " + string.Join(", ", suggestions) + "\n\n현재 태그에 추가할까요?";
            if (MessageBox.Show(this, message, "태그 추천", MessageBoxButtons.YesNo, MessageBoxIcon.Information) == DialogResult.Yes)
            {
                _editTags.Text = string.Join(", ", ParseTags(_editTags.Text).Concat(suggestions).Distinct(StringComparer.OrdinalIgnoreCase));
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception error)
        {
            ReportConnectionFailure(error);
        }
    }

    private void CancelEdit()
    {
        if (!ConfirmDiscardChanges())
        {
            return;
        }

        _isEditing = false;
        if (_activeDocument is not null)
        {
            _ = LoadDocumentAsync(_activeDocument.Slug);
        }
        else
        {
            ShowWelcome();
        }
    }

    private void EditorContentKeyDown(object? sender, KeyEventArgs eventArgs)
    {
        if (!eventArgs.Control || eventArgs.KeyCode != Keys.V || _editContent is null)
        {
            return;
        }

        var attached = false;
        var hasImagePayload = false;
        try
        {
            // Prefer a dropped source file when Windows exposes one: this
            // keeps a .webp untouched. Clipboard bitmap data is encoded as
            // WebP, with a PNG fallback when the local codec cannot encode.
            attached = TryAttachClipboardFiles();
            hasImagePayload = Clipboard.ContainsImage();
            if (!attached && hasImagePayload)
            {
                using var image = Clipboard.GetImage();
                if (image is not null)
                {
                    InsertAttachment(_attachments.SaveClipboardImage(image));
                    attached = true;
                }
            }
        }
        catch (Exception error)
        {
            ShowAttachmentFailure(error);
        }

        // Suppress the ordinary textbox paste even after a failed image save;
        // otherwise Windows may paste an unrelated text representation of the
        // image after reporting an attachment failure.
        if (attached || hasImagePayload)
        {
            eventArgs.SuppressKeyPress = true;
            eventArgs.Handled = true;
        }
    }

    private void EditorContentDragEnter(object? sender, DragEventArgs eventArgs)
    {
        eventArgs.Effect = GetDroppedImagePaths(eventArgs.Data).Any()
            ? DragDropEffects.Copy
            : DragDropEffects.None;
    }

    private void EditorContentDragDrop(object? sender, DragEventArgs eventArgs)
    {
        var failures = new List<string>();
        var count = 0;
        foreach (var path in GetDroppedImagePaths(eventArgs.Data))
        {
            try
            {
                InsertAttachment(_attachments.CopyDroppedImage(path));
                count++;
            }
            catch (Exception error)
            {
                failures.Add(FriendlyError(error));
            }
        }

        if (failures.Count > 0)
        {
            MessageBox.Show(this, string.Join(Environment.NewLine, failures.Distinct()), "이미지 첨부 실패", MessageBoxButtons.OK, MessageBoxIcon.Warning);
        }
        else if (count == 0)
        {
            MessageBox.Show(this, "PNG, JPEG, GIF, BMP 또는 WebP 이미지 파일만 첨부할 수 있습니다.", "이미지 첨부", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }
    }

    private bool TryAttachClipboardFiles()
    {
        try
        {
            var paths = GetDroppedImagePaths(Clipboard.GetDataObject()).ToArray();
            if (paths.Length == 0)
            {
                return false;
            }

            var attached = false;
            var failures = new List<string>();
            foreach (var path in paths)
            {
                try
                {
                    InsertAttachment(_attachments.CopyDroppedImage(path));
                    attached = true;
                }
                catch (Exception error)
                {
                    failures.Add(FriendlyError(error));
                }
            }

            if (failures.Count > 0)
            {
                MessageBox.Show(this, string.Join(Environment.NewLine, failures.Distinct()), "이미지 첨부 실패", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            }

            return attached;
        }
        catch (Exception error)
        {
            ShowAttachmentFailure(error);
            return false;
        }
    }

    private static IEnumerable<string> GetDroppedImagePaths(IDataObject? data)
    {
        if (data?.GetDataPresent(DataFormats.FileDrop) != true
            || data.GetData(DataFormats.FileDrop) is not string[] paths)
        {
            return Array.Empty<string>();
        }

        return paths.Where(IsSupportedDroppedImagePath);
    }

    private static bool IsSupportedDroppedImagePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return false;
        }

        var extension = Path.GetExtension(path);
        return extension.Equals(".png", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".jpg", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".jpeg", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".gif", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".bmp", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".webp", StringComparison.OrdinalIgnoreCase);
    }

    private void InsertAttachment(ClientImageAttachment attachment)
    {
        if (_editContent is null)
        {
            return;
        }

        _editContent.SelectedText = ClipboardImageAttachments.ToWikiImageSyntax(attachment);
        _attachmentsForCurrentEdit.Add(attachment);
        SetStatus($"이미지를 임시 첨부했습니다: {attachment.RelativePath.Replace('\\', '/')}");
    }

    private void ShowAttachmentFailure(Exception error) => MessageBox.Show(
        this,
        $"이미지를 실행 파일 옆 클라이언트 날짜 폴더에 저장하지 못했습니다.\n{FriendlyError(error)}",
        "이미지 첨부 실패",
        MessageBoxButtons.OK,
        MessageBoxIcon.Warning);

    private void ShowAttachmentCopyInstructions(IReadOnlyList<ClientImageAttachment> attachments)
    {
        if (attachments.Count == 0)
        {
            return;
        }

        var folders = attachments
            .Select(attachment => attachment.DateFolder)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(folder => folder, StringComparer.Ordinal)
            .ToArray();
        var fileList = string.Join(Environment.NewLine, attachments.Select(attachment =>
            $"• {attachment.RelativePath.Replace('\\', '/')} ({Path.GetExtension(attachment.FileName).ToLowerInvariant()})"));
        var folderList = string.Join(Environment.NewLine, folders.Select(folder =>
            $"• {Path.Combine(_attachments.Root, folder)}  →  PersonalWiki의 img\\{folder}"));
        MessageBox.Show(this,
            "문서는 저장됐지만, 첨부 이미지는 아직 PersonalWiki 서버에 복사되지 않았습니다.\n\n"
            + "이번 편집에서 만든 파일:\n" + fileList + "\n\n"
            + "아래 클라이언트 날짜 폴더를 PersonalWiki 서버의 해당 img 날짜 폴더로 직접 복사하세요:\n"
            + folderList + "\n\n"
            + "클라이언트는 서버 파일·SQLite에 직접 접근하지 않으므로 자동 복사나 업로드를 하지 않습니다. 클립보드 비트맵은 WebP로 저장되고, WebP 인코딩에 실패할 때만 PNG로 보존됩니다. 파일로 붙인 WebP는 원본 WebP로 보존됩니다.",
            "이미지 폴더 복사 필요",
            MessageBoxButtons.OK,
            MessageBoxIcon.Information);
    }

    private async Task DeleteActiveDocumentAsync()
    {
        if (_activeDocument is null || _activeSlug is null)
        {
            return;
        }

        var answer = MessageBox.Show(this,
            $"‘{_activeDocument.Title}’ 문서를 삭제할까요? 이 작업은 서버의 문서 삭제와 동일하게 되돌릴 수 없습니다.",
            "문서 삭제", MessageBoxButtons.YesNo, MessageBoxIcon.Warning);
        if (answer != DialogResult.Yes)
        {
            return;
        }

        _documentScrollPositionBeforeEditor = null;
        var token = BeginOperation();
        try
        {
            await _api.DeleteDocumentAsync(_activeSlug, token);
            _activeDocument = null;
            _activeSlug = null;
            ShowWelcome();
            await ReloadDocumentsAsync();
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception error)
        {
            ReportConnectionFailure(error);
        }
    }

    private async Task EditSettingsAsync()
    {
        using var dialog = new SettingsDialog(_settings);
        if (dialog.ShowDialog(this) != DialogResult.OK || dialog.Settings is null)
        {
            return;
        }

        var next = ClientSettings.Normalize(dialog.Settings);
        if (next == _settings)
        {
            return;
        }

        // Applying either connection or font settings rebuilds the current
        // view. Do not let that replacement silently discard an editor draft.
        if (_isEditing && !ConfirmDiscardChanges())
        {
            return;
        }

        var portChanged = next.ServerPort != _settings.ServerPort;
        try
        {
            next.Save();
        }
        catch (Exception error)
        {
            MessageBox.Show(this, error.Message, "클라이언트 설정 저장 실패", MessageBoxButtons.OK, MessageBoxIcon.Error);
            return;
        }

        _settings = next;
        // The user explicitly confirmed replacement above. Clear edit mode
        // only after settings persistence succeeds, so a failed save leaves
        // the draft fully active.
        _isEditing = false;
        _documentScrollPositionBeforeEditor = null;
        ApplyClientFont();
        if (portChanged)
        {
            _api.Dispose();
            _api = new ClientApi(_settings.ServerPort);
            _activeDocument = null;
            _activeSlug = null;
            ShowWelcome();
            await ReloadDocumentsAsync();
        }
        else if (_activeDocument is not null)
        {
            await LoadDocumentAsync(_activeDocument.Slug);
        }
        else
        {
            ShowWelcome();
        }

        UpdateEndpointLabel();
    }

    private void ApplyClientFont()
    {
        var previous = _documentFont;
        var previousTitleFont = _titleLabel.Font;
        _documentFont = CreateClientFont(_settings.FontFamily, 10.5f);
        Font = _documentFont;
        _titleLabel.Font = new Font(_documentFont.FontFamily, 17, FontStyle.Bold);
        previousTitleFont.Dispose();
        previous.Dispose();
    }

    private void ShowWelcome()
    {
        _isEditing = false;
        SetHeader("문서를 선택하세요", [], null, false);
        ClearWorkspace();
        var message = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 1,
            Padding = new Padding(32),
            BackColor = Color.FromArgb(255, 250, 240),
            Anchor = AnchorStyles.Top | AnchorStyles.Left,
            Location = new Point(28, 28),
        };
        message.Controls.Add(new Label
        {
            AutoSize = true,
            Text = "PersonalWikiClient",
            Font = new Font(_documentFont.FontFamily, 18, FontStyle.Bold),
        }, 0, 0);
        message.Controls.Add(new Label
        {
            AutoSize = true,
            MaximumSize = new Size(620, 0),
            Text = "이 앱은 PersonalWiki 서버의 localhost JSON API만 사용합니다. 서버 설정 파일이나 SQLite 데이터베이스에 직접 접근하지 않습니다.",
            Margin = new Padding(0, 10, 0, 12),
        }, 0, 1);
        var retry = new Button { AutoSize = true, Text = "연결 다시 시도" };
        retry.Click += async (_, _) => await ReloadDocumentsAsync();
        message.Controls.Add(retry, 0, 2);
        _workspace.Controls.Add(message);
    }

    private void ShowConnectionFailure(Exception error)
    {
        if (IsDisposed || error is ObjectDisposedException)
        {
            return;
        }

        _isEditing = false;
        SetHeader("서버에 연결할 수 없습니다", [], null, false);
        ClearWorkspace();
        var panel = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 1,
            Padding = new Padding(32),
            BackColor = Color.FromArgb(255, 250, 240),
            Anchor = AnchorStyles.Top | AnchorStyles.Left,
            Location = new Point(28, 28),
        };
        panel.Controls.Add(new Label
        {
            AutoSize = true,
            Text = "PersonalWiki 서버가 응답하지 않습니다.",
            Font = new Font(_documentFont.FontFamily, 14, FontStyle.Bold),
        }, 0, 0);
        panel.Controls.Add(new Label
        {
            AutoSize = true,
            MaximumSize = new Size(680, 0),
            Text = $"127.0.0.1:{_settings.ServerPort}에서 PersonalWiki를 실행한 뒤 다시 시도하세요. 이 클라이언트는 원격 주소나 SQLite 파일을 열지 않습니다.\n\n{FriendlyError(error)}",
            Margin = new Padding(0, 10, 0, 12),
        }, 0, 1);
        var buttons = new FlowLayoutPanel { AutoSize = true, AutoSizeMode = AutoSizeMode.GrowAndShrink };
        var retry = new Button { AutoSize = true, Text = "다시 시도" };
        var settings = new Button { AutoSize = true, Text = "포트·글꼴 설정" };
        retry.Click += async (_, _) => await ReloadDocumentsAsync();
        settings.Click += async (_, _) => await EditSettingsAsync();
        buttons.Controls.AddRange([retry, settings]);
        panel.Controls.Add(buttons, 0, 2);
        _workspace.Controls.Add(panel);
        SetStatus("서버 연결 실패");
    }

    /// <summary>
    /// A failed editor-side request must never replace the edit controls: doing so
    /// would silently discard an unsaved title, tag list, or document body.
    /// </summary>
    private void ReportConnectionFailure(Exception error)
    {
        if (IsDisposed || error is ObjectDisposedException)
        {
            return;
        }

        if (_isEditing
            && _editTitle is not null && !_editTitle.IsDisposed
            && _editTags is not null && !_editTags.IsDisposed
            && _editContent is not null && !_editContent.IsDisposed)
        {
            var message = FriendlyError(error);
            SetStatus($"서버 요청 실패 — 편집 내용은 유지되었습니다. {message}");
            MessageBox.Show(
                this,
                $"서버 요청에 실패했습니다. 편집 중인 제목, 태그, 본문은 그대로 유지됩니다.\n\n{message}",
                "서버 요청 실패",
                MessageBoxButtons.OK,
                MessageBoxIcon.Warning);
            return;
        }

        ShowConnectionFailure(error);
    }

    private void SetHeader(string title, IReadOnlyList<string> tags, string? updatedAt, bool canEdit)
    {
        _titleLabel.Text = title;
        _metadataLabel.Text = string.IsNullOrWhiteSpace(updatedAt) ? string.Empty : $"수정: {updatedAt}";
        _editButton.Enabled = canEdit;
        _deleteButton.Enabled = canEdit;
        // ControlCollection.Clear only detaches child controls. Explicitly
        // dispose the old LinkLabels so repeated document navigation cannot
        // accumulate delayed GDI/window-handle cleanup.
        foreach (Control control in _tagBar.Controls.Cast<Control>().ToArray())
        {
            control.Dispose();
        }

        _tagBar.Controls.Clear();
        foreach (var tag in tags)
        {
            var tagLink = new LinkLabel
            {
                AutoSize = true,
                Text = "#" + tag,
                Margin = new Padding(0, 0, 9, 0),
            };
            tagLink.LinkClicked += async (_, _) => await ShowTagDocumentsAsync(tag);
            _tagBar.Controls.Add(tagLink);
        }
    }

    private async Task ShowTagDocumentsAsync(string tag)
    {
        var token = BeginOperation();
        try
        {
            var documents = await _api.GetTagDocumentsAsync(tag, token);
            if (!token.IsCancellationRequested)
            {
                _searchBox.Text = string.Empty;
                PopulateDocumentList(documents);
                SetStatus($"#{tag}: {documents.Count}개 문서");
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception error)
        {
            ReportConnectionFailure(error);
        }
    }

    private void PopulateDocumentList(IEnumerable<DocumentSummary> documents)
    {
        _suppressSelection = true;
        try
        {
            _documentList.BeginUpdate();
            _documentList.Items.Clear();
            // AddRange performs a single collection update for a large wiki;
            // BeginUpdate alone only suppresses the visible repaint.
            _documentList.Items.AddRange(documents.Select(document => new DocumentListItem(document)).ToArray());
        }
        finally
        {
            _documentList.EndUpdate();
            _suppressSelection = false;
        }

        SelectDocumentInList(_activeSlug);
    }

    private void SelectDocumentInList(string? slug)
    {
        if (string.IsNullOrWhiteSpace(slug))
        {
            return;
        }

        _suppressSelection = true;
        try
        {
            for (var index = 0; index < _documentList.Items.Count; index++)
            {
                if (_documentList.Items[index] is DocumentListItem item
                    && string.Equals(item.Document.Slug, slug, StringComparison.OrdinalIgnoreCase))
                {
                    _documentList.SelectedIndex = index;
                    return;
                }
            }
        }
        finally
        {
            _suppressSelection = false;
        }
    }

    private RendererActions CreateRendererActions() => new()
    {
        OpenDocumentAsync = LoadDocumentAsync,
        CreateDocumentAsync = title =>
        {
            BeginNewDocument(title);
            return Task.CompletedTask;
        },
        OpenExternalAsync = OpenExternalAsync,
        DownloadFileAsync = DownloadFileAsync,
        LoadLocalImageAsync = LoadLocalImageAsync,
        LoadRemoteImageAsync = LoadRemoteImageAsync,
    };

    private async Task<Image?> LoadLocalImageAsync(string source, CancellationToken renderCancellation)
    {
        var normalized = source.Trim().TrimStart('/');
        if (!normalized.StartsWith("img/", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        try
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token, renderCancellation);
            var bytes = await _api.GetAssetBytesUpToAsync(normalized, MaxLocalImageBytes, cancellation.Token);
            return DecodeImage(bytes);
        }
        catch (Exception) when (!_lifetime.IsCancellationRequested)
        {
            return null;
        }
    }

    private async Task<Image?> LoadRemoteImageAsync(Uri source, CancellationToken renderCancellation)
    {
        if (source.Scheme is not ("http" or "https"))
        {
            return null;
        }

        try
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token, renderCancellation);
            using var response = await _remoteImageHttp.GetAsync(source, HttpCompletionOption.ResponseHeadersRead, cancellation.Token);
            if (!response.IsSuccessStatusCode || response.Content.Headers.ContentLength is > MaxRemoteImageBytes)
            {
                return null;
            }

            await using var stream = await response.Content.ReadAsStreamAsync(cancellation.Token);
            await using var copied = new MemoryStream();
            var buffer = new byte[81920];
            var total = 0;
            while (true)
            {
                var count = await stream.ReadAsync(buffer, cancellation.Token);
                if (count == 0)
                {
                    break;
                }

                total += count;
                if (total > MaxRemoteImageBytes)
                {
                    return null;
                }

                await copied.WriteAsync(buffer.AsMemory(0, count), cancellation.Token);
            }

            return DecodeImage(copied.ToArray());
        }
        catch (Exception) when (!_lifetime.IsCancellationRequested)
        {
            return null;
        }
    }

    private static Image? DecodeImage(byte[] bytes)
    {
        return NativeImageDecoder.Decode(bytes);
    }

    private async Task DownloadFileAsync(string relativePath)
    {
        var normalized = relativePath.Trim().TrimStart('/');
        if (!normalized.StartsWith("file/", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        using var dialog = new SaveFileDialog
        {
            FileName = Path.GetFileName(normalized),
            Title = "첨부 파일 저장",
            OverwritePrompt = true,
        };
        if (dialog.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        try
        {
            SetStatus("첨부 파일을 받는 중…");
            var bytes = await _api.GetAssetBytesAsync(normalized, _lifetime.Token);
            await File.WriteAllBytesAsync(dialog.FileName, bytes, _lifetime.Token);
            SetStatus("첨부 파일을 저장했습니다.");
        }
        catch (Exception error) when (!_lifetime.IsCancellationRequested)
        {
            MessageBox.Show(this, FriendlyError(error), "파일 저장 실패", MessageBoxButtons.OK, MessageBoxIcon.Warning);
        }
    }

    private static Task OpenExternalAsync(string url)
    {
        if (!RendererActions.TryGetAllowedExternalUri(url, out var target))
        {
            return Task.CompletedTask;
        }

        try
        {
            Process.Start(new ProcessStartInfo(target.AbsoluteUri) { UseShellExecute = true });
        }
        catch (System.ComponentModel.Win32Exception)
        {
            // Do not fail document rendering if Windows has no browser association.
        }

        return Task.CompletedTask;
    }

    private CancellationToken BeginOperation()
    {
        _operation?.Cancel();
        _operation?.Dispose();
        _operation = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        return _operation.Token;
    }

    private bool ConfirmDiscardChanges() => !_isEditing
        || MessageBox.Show(this, "저장하지 않은 변경을 버릴까요?", "편집 취소", MessageBoxButtons.YesNo, MessageBoxIcon.Warning) == DialogResult.Yes;

    private void UpdateEndpointLabel() => _endpointLabel.Text = $"로컬 전용\n127.0.0.1:{_settings.ServerPort}";

    private void SetStatus(string message) => _statusLabel.Text = message;

    private void ClearWorkspace()
    {
        _documentRenderer = null;
        foreach (Control control in _workspace.Controls.Cast<Control>().ToArray())
        {
            control.Dispose();
        }

        _workspace.Controls.Clear();
    }

    private static IReadOnlyList<string> ParseTags(string text) => text
        .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();

    private static Font CreateClientFont(string family, float size)
    {
        try
        {
            return new Font(family, size, FontStyle.Regular, GraphicsUnit.Point);
        }
        catch (ArgumentException)
        {
            return new Font("Segoe UI", size, FontStyle.Regular, GraphicsUnit.Point);
        }
    }

    private static string FriendlyError(Exception error) => error switch
    {
        ClientApiException apiError => apiError.Message,
        HttpRequestException => "서버에 연결하지 못했습니다.",
        TaskCanceledException => "서버 응답 시간이 초과되었습니다.",
        _ => error.Message,
    };

    private void DisposeResources()
    {
        _searchTimer.Stop();
        _operation?.Cancel();
        _operation?.Dispose();
        _lifetime.Cancel();
        _lifetime.Dispose();
        _remoteImageHttp.Dispose();
        _api.Dispose();
        _documentFont.Dispose();
        FontCache.DisposeCachedFonts();
    }

    private sealed record DocumentScrollRestore(string Slug, Point Position);

    private sealed record DocumentListItem(DocumentSummary Document)
    {
        public string Text => Document.Title;
    }
}
