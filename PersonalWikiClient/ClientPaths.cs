namespace PersonalWikiClient;

/// <summary>
/// The desktop client has no dependency on the server installation directory.
/// Its own small preference file is intentionally kept in LocalAppData.
/// </summary>
internal static class ClientPaths
{
    private static readonly string Root = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "PersonalWikiClient");

    public static string SettingsPath => Path.Combine(Root, "settings.json");
}
