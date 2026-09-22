using System.Drawing.Text;

namespace PersonalWikiClient;

/// <summary>
/// Client-only preferences. This dialog does not read or write the server's
/// wikisettings.cfg and accepts only a TCP port for 127.0.0.1.
/// </summary>
internal sealed class SettingsDialog : Form
{
    private readonly NumericUpDown _port = new()
    {
        Minimum = 1,
        Maximum = 65535,
        Width = 140,
    };
    private readonly ComboBox _font = new()
    {
        DropDownStyle = ComboBoxStyle.DropDown,
        Width = 280,
    };

    public SettingsDialog(ClientSettings current)
    {
        Text = "PersonalWikiClient 설정";
        StartPosition = FormStartPosition.CenterParent;
        FormBorderStyle = FormBorderStyle.FixedDialog;
        MaximizeBox = false;
        MinimizeBox = false;
        ShowInTaskbar = false;
        ClientSize = new Size(525, 282);

        var body = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            Padding = new Padding(18),
        };
        body.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        body.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        body.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        body.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        body.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        body.RowStyles.Add(new RowStyle(SizeType.AutoSize));

        body.Controls.Add(new Label
        {
            AutoSize = true,
            MaximumSize = new Size(465, 0),
            Text = "이 설정은 PersonalWikiClient 전용입니다. PersonalWiki의 포트 설정, 웹 브라우저 글꼴, SQLite 데이터는 변경하지 않습니다.",
            Margin = new Padding(0, 0, 0, 12),
        }, 0, 0);
        body.Controls.Add(Field("PersonalWiki localhost 포트 (1–65535)", _port), 0, 1);
        body.Controls.Add(Field("클라이언트 표시 글꼴", _font), 0, 2);
        body.Controls.Add(new Label
        {
            AutoSize = true,
            MaximumSize = new Size(465, 0),
            Text = "항상 127.0.0.1에만 연결합니다. 외부 IP, 도메인, ngrok, Tailscale 주소는 입력하거나 연결할 수 없습니다.",
            ForeColor = Color.DimGray,
            Margin = new Padding(0, 13, 0, 0),
        }, 0, 3);
        var buttons = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            FlowDirection = FlowDirection.RightToLeft,
            Dock = DockStyle.Fill,
            Margin = new Padding(0, 12, 0, 0),
        };
        var ok = new Button { Text = "저장", AutoSize = true, DialogResult = DialogResult.None };
        var cancel = new Button { Text = "취소", AutoSize = true, DialogResult = DialogResult.Cancel };
        ok.Click += (_, _) => SaveAndClose();
        buttons.Controls.AddRange([ok, cancel]);
        body.Controls.Add(buttons, 0, 4);
        Controls.Add(body);

        AcceptButton = ok;
        CancelButton = cancel;
        _port.Value = Math.Clamp(current.ServerPort, (int)_port.Minimum, (int)_port.Maximum);
        PopulateFonts();
        _font.Text = ClientSettings.NormalizeFontFamily(current.FontFamily);
    }

    public ClientSettings? Settings { get; private set; }

    private static Control Field(string label, Control input)
    {
        var panel = new TableLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            ColumnCount = 1,
            Dock = DockStyle.Top,
            Margin = new Padding(0, 0, 0, 10),
        };
        panel.Controls.Add(new Label { AutoSize = true, Text = label, Margin = new Padding(0, 0, 0, 3) }, 0, 0);
        panel.Controls.Add(input, 0, 1);
        return panel;
    }

    private void PopulateFonts()
    {
        try
        {
            using var fonts = new InstalledFontCollection();
            _font.Items.AddRange(fonts.Families
                .Select(family => family.Name)
                .OrderBy(name => name, StringComparer.CurrentCultureIgnoreCase)
                .Cast<object>()
                .ToArray());
        }
        catch (Exception)
        {
            // A manually entered family remains valid with WinForms fallback.
        }
    }

    private void SaveAndClose()
    {
        var family = ClientSettings.NormalizeFontFamily(_font.Text);
        Settings = new ClientSettings
        {
            ServerPort = decimal.ToInt32(_port.Value),
            FontFamily = family,
        };
        DialogResult = DialogResult.OK;
        Close();
    }
}
