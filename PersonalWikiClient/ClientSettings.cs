using System.Text.Json;

namespace PersonalWikiClient;

internal sealed record ClientSettings
{
    public const int DefaultServerPort = 6885;
    public const string DefaultFontFamily = "Noto Sans KR";

    public int ServerPort { get; init; } = DefaultServerPort;
    public string FontFamily { get; init; } = DefaultFontFamily;

    public static ClientSettings Load()
    {
        try
        {
            if (!File.Exists(ClientPaths.SettingsPath))
            {
                return new ClientSettings();
            }

            return Normalize(JsonSerializer.Deserialize<ClientSettings>(File.ReadAllText(ClientPaths.SettingsPath)));
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException)
        {
            return new ClientSettings();
        }
    }

    public static ClientSettings Normalize(ClientSettings? source)
    {
        var port = source?.ServerPort is >= 1 and <= 65535
            ? source.ServerPort
            : DefaultServerPort;
        return new ClientSettings
        {
            ServerPort = port,
            FontFamily = NormalizeFontFamily(source?.FontFamily),
        };
    }

    public static string NormalizeFontFamily(string? value)
    {
        var font = (value ?? string.Empty).Trim();
        if (font.Length is 0 or > 128 || font.Any(char.IsControl))
        {
            return DefaultFontFamily;
        }

        return font;
    }

    public void Save()
    {
        var directory = Path.GetDirectoryName(ClientPaths.SettingsPath)
            ?? throw new InvalidOperationException("클라이언트 설정 폴더를 찾을 수 없습니다.");
        Directory.CreateDirectory(directory);
        var temporary = ClientPaths.SettingsPath + ".tmp";
        File.WriteAllText(
            temporary,
            JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true }));
        File.Move(temporary, ClientPaths.SettingsPath, true);
    }
}
