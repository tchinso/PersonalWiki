using System.Reflection;
using System.Text.Json;

namespace PersonalWikiClient;

internal static class Program
{
    [STAThread]
    private static int Main()
    {
        try
        {
            ApplicationConfiguration.Initialize();
            var fixturePath = Path.Combine(AppContext.BaseDirectory, "native-renderer-fixture.json");
            using var fixture = JsonDocument.Parse(File.ReadAllText(fixturePath));
            var payload = DocumentPayload.FromJson(fixture.RootElement);
            RunNullableNumberSmokeTest();
            RunAttachmentStoreSmokeTest();
            RunWebpDecodeSmokeTest();
            RunImageLoadConcurrencySmokeTest();
            Assert(RendererActions.TryGetAllowedExternalUri("https%3A%2F%2Fexample.com%2Fcanonical", out var canonical)
                && canonical.AbsoluteUri == "https://example.com/canonical", "canonical external URL dispatch");
            Assert(!RendererActions.TryGetAllowedExternalUri("shell:AppsFolder", out _), "dangerous protocol validation");
            Assert(!RendererActions.TryGetAllowedExternalUri("file:///C:/sensitive.txt", out _), "file URL validation");
            Assert(!RendererActions.TryGetAllowedExternalUri("mailto:test@example.com", out _), "mailto URL validation");
            Assert(!RendererActions.TryGetAllowedExternalUri("#section-one", out _), "fragment remains native");
            var openedDocuments = new List<string>();
            var createdDocuments = new List<string>();
            var openedUrls = new List<string>();
            using var host = new Form { Size = new Size(1024, 768) };
            using var renderer = new NativeMarkdownRenderer { Dock = DockStyle.Fill };
            host.Controls.Add(renderer);
            host.CreateControl();
            renderer.CreateControl();
            renderer.Render(payload.Ast, new RendererActions
            {
                OpenDocumentAsync = slug =>
                {
                    openedDocuments.Add(slug);
                    return Task.CompletedTask;
                },
                CreateDocumentAsync = title =>
                {
                    createdDocuments.Add(title);
                    return Task.CompletedTask;
                },
                OpenExternalAsync = url =>
                {
                    openedUrls.Add(url);
                    return Task.CompletedTask;
                },
                LoadLocalImageAsync = (_, _) => Task.FromResult<Image?>(null),
                LoadRemoteImageAsync = (_, _) => Task.FromResult<Image?>(null),
            }, new Font("Segoe UI", 10));

            var controls = Descendants(renderer).ToArray();
            Assert(HasText<Label>(controls, "문법 종합 검사"), "heading");
            Assert(controls.OfType<Label>().Any(label => label.Text == "기울임" && label.Font.Italic), "italic");
            Assert(controls.OfType<Label>().Any(label => label.Text == "굵게" && label.Font.Bold), "strong");
            Assert(controls.OfType<Label>().Any(label => label.Text == "취소" && label.Font.Strikeout), "strikethrough");
            Assert(controls.OfType<Label>().Any(label => label.Text == "강조" && label.BackColor.R > 240 && label.BackColor.G > 200), "highlight");
            Assert(controls.OfType<LinkLabel>().Any(label => label.Text.StartsWith("[스포일러", StringComparison.Ordinal)), "spoiler");
            Assert(controls.OfType<TextBox>().Any(box => box.ReadOnly && box.Text.Contains("print('code')", StringComparison.Ordinal)), "fenced code");
            Assert(controls.OfType<Label>().Any(label => label.Text is "☐" or "☑"), "task list");
            Assert(HasText<Label>(controls, "하위 항목") && controls.OfType<Label>().Any(label => label.Text == "1."), "nested and ordered list");
            Assert(HasText<Label>(controls, "중첩 인용"), "nested quote");
            Assert(controls.OfType<TableLayoutPanel>().Any(table => table.CellBorderStyle == TableLayoutPanelCellBorderStyle.Single), "table");
            Assert(controls.OfType<TableLayoutPanel>().Any(table => Equals(table.Tag, "center")), "table center align");
            Assert(controls.OfType<TableLayoutPanel>().Any(table => Equals(table.Tag, "right")), "table right align");
            Assert(controls.OfType<TableLayoutPanel>().Any(table => table.Controls.Cast<Control>().Any(control => table.GetColumnSpan(control) > 1)), "table colspan/multi-layout");
            Assert(HasText<Label>(controls, "노트 콜아웃") && HasText<Label>(controls, "콜아웃 첫 줄") && HasText<Label>(controls, "경고 콜아웃") && HasText<Label>(controls, "위험 콜아웃"), "all callout kinds");
            Assert(controls.OfType<PictureBox>().Any(box => box.AccessibleName == "로컬 이미지"), "PersonalWiki image");
            Assert(controls.OfType<PictureBox>().Any(box => box.AccessibleName == "외부 이미지"), "Markdown image");
            Assert(controls.OfType<PictureBox>().Any(box => box.AccessibleName == "YouTube thumbnail"), "YouTube native card");
            Assert(controls.OfType<Button>().Any(button => button.Text.StartsWith("▶ Template: 접는 템플릿", StringComparison.Ordinal)), "folded template");
            Assert(HasText<Label>(controls, "인라인 템플릿 본문"), "inline template");
            Assert(controls.OfType<FlowLayoutPanel>().Any(panel => panel.Controls.OfType<Label>().Any(label => label.Text == "목차")), "TOC");
            Assert(controls.OfType<LinkLabel>().Any(label => label.Text == "[1]"), "footnote reference");
            Assert(HasText<Label>(controls, "각주 내용"), "footnote definition");
            Assert(controls.OfType<Label>().Any(label => label.AccessibleName == "keyboard key" && label.Text == "Ctrl"), "safe kbd HTML");
            Assert(HasText<Label>(controls, "<script>literal</script>"), "safe raw HTML text");

            var anchors = GetAnchors(renderer);
            Assert(anchors.Contains("section-one") && anchors.Contains("footnote-1") && anchors.Contains("footnote-ref-1"), "TOC/footnote anchors");

            var tagLink = controls.OfType<LinkLabel>().Single(label => label.Text == "알려진 문서");
            InvokeLink(tagLink);
            Assert(openedDocuments.Contains("known-slug", StringComparer.Ordinal), "tag embed uses URL slug, not title target");

            var wikiLink = controls.OfType<LinkLabel>().Single(label => label.Text == "위키 문서");
            InvokeLink(wikiLink);
            Assert(openedDocuments.Contains("known-slug", StringComparer.Ordinal), "wiki link");

            var anchorLink = controls.OfType<LinkLabel>().Single(label => label.Text == "섹션으로");
            InvokeLink(anchorLink);
            Assert(!openedUrls.Any(url => url.StartsWith('#')), "local heading anchor stays native");

            var newLink = controls.OfType<LinkLabel>().Single(label => label.Text == "새 문서");
            InvokeLink(newLink);
            Assert(createdDocuments.Contains("새 문서", StringComparer.Ordinal), "decoded new-document title");

            var malformedDocumentLink = controls.OfType<LinkLabel>().Single(label => label.Text == "잘못된 문서 링크");
            InvokeLink(malformedDocumentLink);
            var malformedNewLink = controls.OfType<LinkLabel>().Single(label => label.Text == "잘못된 새 문서");
            InvokeLink(malformedNewLink);
            Assert(!openedDocuments.Any(slug => slug.Contains('%') || slug.Any(char.IsControl)), "malformed document URL is ignored");
            Assert(!createdDocuments.Any(title => title.Any(char.IsControl)), "malformed new-document URL is ignored");

            var youtubeLink = controls.OfType<LinkLabel>().Single(label => label.Text.Contains("기본 브라우저에서 열기", StringComparison.Ordinal));
            InvokeLink(youtubeLink);
            Assert(openedUrls.Any(url => url.Contains("youtube.com/watch", StringComparison.Ordinal)), "YouTube external action");

            var dangerousLink = controls.OfType<LinkLabel>().Single(label => label.Text == "위험 링크");
            InvokeLink(dangerousLink);
            Assert(!openedUrls.Any(url => url.StartsWith("ms-settings:", StringComparison.OrdinalIgnoreCase)), "dangerous protocol is not dispatched");

            Console.WriteLine("Native renderer and attachment smoke test passed: documented syntax controls, TOC, tag slug, footnotes, PNG/WebP attachments and valid WebP decoding.");
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error);
            return 1;
        }
    }

    private static void RunNullableNumberSmokeTest()
    {
        using var terminalPage = JsonDocument.Parse("{\"next_offset\":null}");
        Assert(JsonValue.Int(terminalPage.RootElement, "next_offset") is null,
            "terminal pagination accepts a null next_offset");
    }

    private static void RunAttachmentStoreSmokeTest()
    {
        var root = Path.Combine(Path.GetTempPath(), "personalwiki-client-smoke-" + Guid.NewGuid().ToString("N"));
        try
        {
            var store = new ClipboardImageAttachments(root, () => new DateTime(2026, 9, 22, 17, 30, 0));
            using var bitmap = new Bitmap(3, 3);
            using (var graphics = Graphics.FromImage(bitmap))
            {
                graphics.Clear(Color.MediumPurple);
            }

            var clipboard = store.SaveClipboardImage(bitmap);
            Assert(clipboard.DateFolder == "20260922" && clipboard.FileName.EndsWith(".webp", StringComparison.OrdinalIgnoreCase), "clipboard WebP date attachment");
            Assert(File.Exists(clipboard.AbsolutePath), "clipboard attachment file");
            Assert(ClipboardImageAttachments.ToWikiImageSyntax(clipboard).StartsWith("![[20260922/", StringComparison.Ordinal), "clipboard wiki syntax");
            using (var decodedWebp = NativeImageDecoder.Decode(File.ReadAllBytes(clipboard.AbsolutePath)))
            {
                Assert(decodedWebp is not null && decodedWebp.Width == 3 && decodedWebp.Height == 3, "clipboard WebP encode/decode");
            }

            var fallbackStore = new ClipboardImageAttachments(root, () => new DateTime(2026, 9, 22, 17, 30, 0), (_, _) => false);
            var fallback = fallbackStore.SaveClipboardImage(bitmap);
            Assert(fallback.FileName.EndsWith(".png", StringComparison.OrdinalIgnoreCase), "clipboard WebP encoder failure falls back to PNG");
            using (var decodedPng = NativeImageDecoder.Decode(File.ReadAllBytes(fallback.AbsolutePath)))
            {
                Assert(decodedPng is not null && decodedPng.Width == 3 && decodedPng.Height == 3, "PNG fallback remains readable");
            }

            // A camera-sized clipboard bitmap must not retain its full native
            // pixel buffer when the client only renders it inside a 960 px
            // picture box. This also verifies the codec's scaled WebP path.
            using (var largeBitmap = new Bitmap(3000, 2000))
            {
                var largeClipboard = store.SaveClipboardImage(largeBitmap);
                using var bounded = NativeImageDecoder.Decode(File.ReadAllBytes(largeClipboard.AbsolutePath));
                Assert(bounded is not null
                    && bounded.Width <= 2048
                    && bounded.Height <= 2048
                    && (long)bounded.Width * bounded.Height <= 4L * 1024 * 1024,
                    "large clipboard WebP has bounded native decode");
            }

            // The attachment copy itself is bounded before WebP/PNG encoding,
            // rather than first cloning an arbitrarily large clipboard bitmap.
            var constrainedSize = ClipboardImageAttachments.GetBoundedBitmapSize(5000, 4000);
            Assert(constrainedSize.Width <= 4096
                && constrainedSize.Height <= 4096
                && (long)constrainedSize.Width * constrainedSize.Height <= 16L * 1024 * 1024,
                "clipboard encode buffer size is bounded");
            using (var wideBitmap = new Bitmap(4097, 1))
            {
                var constrainedClipboard = store.SaveClipboardImage(wideBitmap);
                using var constrained = NativeImageDecoder.Decode(File.ReadAllBytes(constrainedClipboard.AbsolutePath));
                Assert(constrained is not null && constrained.Width <= 4096,
                    "clipboard encoder applies long-side bound before writing");
            }

            var sourceWebp = Path.Combine(root, "source.webp");
            File.WriteAllBytes(sourceWebp, [0x52, 0x49, 0x46, 0x46, 0x00, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50]);
            var dropped = store.CopyDroppedImage(sourceWebp);
            Assert(dropped.FileName.EndsWith(".webp", StringComparison.OrdinalIgnoreCase), "dropped WebP extension preservation");
            Assert(File.ReadAllBytes(dropped.AbsolutePath).SequenceEqual(File.ReadAllBytes(sourceWebp)), "dropped WebP byte preservation");
            using (var invalidWebp = NativeImageDecoder.Decode(File.ReadAllBytes(dropped.AbsolutePath)))
            {
                Assert(invalidWebp is null, "malformed WebP is isolated from the renderer");
            }
            var referenced = ClipboardImageAttachments.ReferencedInContent(
                ClipboardImageAttachments.ToWikiImageSyntax(clipboard),
                [clipboard, dropped]);
            Assert(referenced.SequenceEqual([clipboard]), "only final-text attachments receive copy reminder");
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    private static void RunWebpDecodeSmokeTest()
    {
        var fixturePath = Path.Combine(AppContext.BaseDirectory, "strawberry.webp");
        Assert(File.Exists(fixturePath), "valid repository WebP fixture is copied for smoke test");

        using var decoded = NativeImageDecoder.Decode(File.ReadAllBytes(fixturePath));
        Assert(decoded is not null && decoded.Width > 0 && decoded.Height > 0, "native WebP decoder displays a valid WebP");
    }

    private static void RunImageLoadConcurrencySmokeTest()
    {
        var inFlight = 0;
        var peak = 0;
        using var host = new Form { Size = new Size(1024, 768) };
        using var renderer = new NativeMarkdownRenderer { Dock = DockStyle.Fill };
        using var documentFont = new Font("Segoe UI", 10);
        host.Controls.Add(renderer);
        host.CreateControl();
        renderer.CreateControl();
        var images = Enumerable.Range(0, 7)
            .Select(index => new AstNode("image", $"img/concurrency-{index}.png", null, []))
            .ToArray();
        renderer.Render(images, new RendererActions
        {
            LoadLocalImageAsync = async (_, cancellationToken) =>
            {
                var active = Interlocked.Increment(ref inFlight);
                while (true)
                {
                    var observed = Volatile.Read(ref peak);
                    if (observed >= active || Interlocked.CompareExchange(ref peak, active, observed) == observed)
                    {
                        break;
                    }
                }

                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                    return null;
                }
                finally
                {
                    Interlocked.Decrement(ref inFlight);
                }
            },
        }, documentFont);

        Assert(Volatile.Read(ref peak) == 4 && Volatile.Read(ref inFlight) == 4,
            "image fetch/decode concurrency is capped per rendered document");
        renderer.ClearDocument();
        PumpUntil(() => Volatile.Read(ref inFlight) == 0,
            "cancelled image fetches release their concurrency slots");
    }

    private static void PumpUntil(Func<bool> condition, string failure)
    {
        var deadline = Environment.TickCount64 + 3000;
        while (!condition())
        {
            if (Environment.TickCount64 >= deadline)
            {
                throw new InvalidOperationException(failure);
            }

            Application.DoEvents();
            Thread.Sleep(10);
        }
    }

    private static IEnumerable<Control> Descendants(Control root)
    {
        yield return root;
        foreach (Control child in root.Controls)
        {
            foreach (var descendant in Descendants(child))
            {
                yield return descendant;
            }
        }
    }

    private static bool HasText<TControl>(IEnumerable<Control> controls, string text)
        where TControl : Control => controls.OfType<TControl>().Any(control => control.Text.Contains(text, StringComparison.Ordinal));

    private static HashSet<string> GetAnchors(NativeMarkdownRenderer renderer)
    {
        var field = typeof(NativeMarkdownRenderer).GetField("_anchors", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Renderer anchor map was not found.");
        var map = field.GetValue(renderer) as IDictionary<string, Control>
            ?? throw new InvalidOperationException("Renderer anchor map has an unexpected type.");
        return map.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static void InvokeLink(LinkLabel link)
    {
        var method = typeof(LinkLabel).GetMethod("OnLinkClicked", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("LinkLabel click hook was not found.");
        method.Invoke(link, [new LinkLabelLinkClickedEventArgs(null)]);
    }

    private static void Assert(bool condition, string coverage)
    {
        if (!condition)
        {
            throw new InvalidOperationException($"Missing native renderer coverage: {coverage}");
        }
    }
}
