namespace PersonalWikiClient;

/// <summary>Temporary native preview used while editing; no HTML is involved.</summary>
internal sealed class PreviewForm : Form
{
    public PreviewForm(IReadOnlyList<AstNode> ast, RendererActions actions, Font documentFont)
    {
        Text = "PersonalWikiClient 네이티브 미리보기";
        StartPosition = FormStartPosition.CenterParent;
        MinimumSize = new Size(680, 460);
        Size = new Size(960, 680);

        var renderer = new NativeMarkdownRenderer { Dock = DockStyle.Fill };
        renderer.Render(ast, actions, documentFont);
        Controls.Add(renderer);
    }
}
