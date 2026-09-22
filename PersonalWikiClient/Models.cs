using System.Text.Json;

namespace PersonalWikiClient;

internal sealed record DocumentSummary(
    string Title,
    string Slug,
    IReadOnlyList<string> Tags,
    string? CreatedAt,
    string? UpdatedAt)
{
    public static DocumentSummary FromJson(JsonElement element) => new(
        JsonValue.String(element, "title") ?? "",
        JsonValue.String(element, "slug") ?? "",
        JsonValue.StringList(element, "tags"),
        JsonValue.String(element, "created_at", "createdAt"),
        JsonValue.String(element, "updated_at", "updatedAt"));
}

internal sealed record WikiDocument(
    string Title,
    string Slug,
    IReadOnlyList<string> Tags,
    string Content,
    string? CreatedAt,
    string? UpdatedAt)
{
    public static WikiDocument FromJson(JsonElement element) => new(
        JsonValue.String(element, "title") ?? "",
        JsonValue.String(element, "slug") ?? "",
        JsonValue.StringList(element, "tags"),
        JsonValue.String(element, "content") ?? "",
        JsonValue.String(element, "created_at", "createdAt"),
        JsonValue.String(element, "updated_at", "updatedAt"));
}

/// <summary>
/// A server-produced PersonalWiki Markdown AST node.  The renderer deliberately
/// receives this structured representation instead of HTML, so the client never
/// embeds a browser engine or parses web markup.
/// </summary>
internal sealed record AstNode(
    string Type,
    string? Text,
    JsonElement? Attributes,
    IReadOnlyList<AstNode> Children)
{
    public static AstNode FromJson(JsonElement element)
    {
        var type = JsonValue.String(element, "type") ?? "text";
        var text = JsonValue.String(element, "text", "raw", "value");
        JsonElement? attributes = null;
        if (element.TryGetProperty("attrs", out var attrs) && attrs.ValueKind == JsonValueKind.Object)
        {
            attributes = attrs.Clone();
        }
        else if (element.TryGetProperty("attributes", out var alternateAttrs) && alternateAttrs.ValueKind == JsonValueKind.Object)
        {
            attributes = alternateAttrs.Clone();
        }

        var children = new List<AstNode>();
        if (element.TryGetProperty("children", out var rawChildren) && rawChildren.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in rawChildren.EnumerateArray())
            {
                children.Add(FromJson(child));
            }
        }

        return new AstNode(type, text, attributes, children);
    }

    public string? AttributeString(params string[] names) =>
        Attributes is { ValueKind: JsonValueKind.Object } attrs
            ? JsonValue.String(attrs, names)
            : null;

    public int? AttributeInt(params string[] names) =>
        Attributes is { ValueKind: JsonValueKind.Object } attrs
            ? JsonValue.Int(attrs, names)
            : null;

    public bool? AttributeBool(params string[] names) =>
        Attributes is { ValueKind: JsonValueKind.Object } attrs
            ? JsonValue.Bool(attrs, names)
            : null;

    public IReadOnlyList<string> AttributeStringList(params string[] names) =>
        Attributes is { ValueKind: JsonValueKind.Object } attrs
            ? JsonValue.StringList(attrs, names)
            : Array.Empty<string>();

    public IReadOnlyList<int> AttributeIntList(params string[] names)
    {
        if (Attributes is not { ValueKind: JsonValueKind.Object } attrs)
        {
            return Array.Empty<int>();
        }

        foreach (var name in names)
        {
            if (!attrs.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            return value.EnumerateArray()
                .Where(item => item.TryGetInt32(out _))
                .Select(item => item.GetInt32())
                .ToArray();
        }

        return Array.Empty<int>();
    }

    public IReadOnlyList<float> AttributeFloatList(params string[] names)
    {
        if (Attributes is not { ValueKind: JsonValueKind.Object } attrs)
        {
            return Array.Empty<float>();
        }

        foreach (var name in names)
        {
            if (!attrs.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            return value.EnumerateArray()
                .Where(item => item.TryGetDouble(out _))
                .Select(item => (float)item.GetDouble())
                .Where(item => item > 0)
                .ToArray();
        }

        return Array.Empty<float>();
    }

    public IReadOnlyList<JsonElement> AttributeObjectList(params string[] names)
    {
        if (Attributes is not { ValueKind: JsonValueKind.Object } attrs)
        {
            return Array.Empty<JsonElement>();
        }

        foreach (var name in names)
        {
            if (!attrs.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            return value.EnumerateArray()
                .Where(item => item.ValueKind == JsonValueKind.Object)
                .Select(item => item.Clone())
                .ToArray();
        }

        return Array.Empty<JsonElement>();
    }

    public IReadOnlyList<AstNode> AttributeNodeList(params string[] names)
    {
        if (Attributes is not { ValueKind: JsonValueKind.Object } attrs)
        {
            return Array.Empty<AstNode>();
        }

        foreach (var name in names)
        {
            if (attrs.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Array)
            {
                return value.EnumerateArray().Select(FromJson).ToArray();
            }
        }

        return Array.Empty<AstNode>();
    }
}

internal sealed record DocumentPayload(
    WikiDocument Document,
    IReadOnlyList<AstNode> Ast,
    IReadOnlyList<DocumentSummary> Backlinks)
{
    public static DocumentPayload FromJson(JsonElement root)
    {
        var document = root.TryGetProperty("document", out var documentValue)
            ? WikiDocument.FromJson(documentValue)
            : WikiDocument.FromJson(root);
        var ast = JsonValue.NodeList(root, "ast", "nodes", "render_ast");
        var backlinks = JsonValue.DocumentList(root, "backlinks");
        return new DocumentPayload(document, ast, backlinks);
    }
}

internal sealed record RenderPayload(IReadOnlyList<AstNode> Ast)
{
    public static RenderPayload FromJson(JsonElement root) =>
        new(JsonValue.NodeList(root, "ast", "nodes", "render_ast"));
}

internal sealed record SaveDocumentRequest(
    string Title,
    string Content,
    IReadOnlyList<string> Tags,
    bool SaveAsIs = false,
    bool IgnoreTagWarning = false);

internal sealed record SaveDocumentResult(
    WikiDocument? Document,
    string? Error,
    string? Warning,
    IReadOnlyList<string> SuggestedTags,
    bool NeedsSpellcheckDecision,
    bool NeedsTagWarningDecision)
{
    public bool IsSuccess => Document is not null && string.IsNullOrEmpty(Error);

    public static SaveDocumentResult FromJson(JsonElement root)
    {
        WikiDocument? document = null;
        if (root.TryGetProperty("document", out var documentValue) && documentValue.ValueKind == JsonValueKind.Object)
        {
            document = WikiDocument.FromJson(documentValue);
        }

        return new SaveDocumentResult(
            document,
            JsonValue.String(root, "error", "message"),
            JsonValue.String(root, "warning", "tag_warning", "spell_warning"),
            JsonValue.StringList(root, "suggested_tags", "suggestions", "recommended_tags"),
            JsonValue.Bool(root, "needs_spellcheck_decision", "needsSpellcheckDecision") ?? false,
            JsonValue.Bool(root, "needs_tag_warning_decision", "needsTagWarningDecision") ?? false);
    }
}

internal static class JsonValue
{
    public static string? String(JsonElement element, params string[] names)
    {
        foreach (var name in names)
        {
            if (!element.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            if (value.ValueKind == JsonValueKind.String)
            {
                return value.GetString();
            }

            if (value.ValueKind is JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False)
            {
                return value.ToString();
            }
        }

        return null;
    }

    public static int? Int(JsonElement element, params string[] names)
    {
        foreach (var name in names)
        {
            if (element.TryGetProperty(name, out var value) && value.TryGetInt32(out var parsed))
            {
                return parsed;
            }
        }

        return null;
    }

    public static bool? Bool(JsonElement element, params string[] names)
    {
        foreach (var name in names)
        {
            if (element.TryGetProperty(name, out var value))
            {
                if (value.ValueKind is JsonValueKind.True or JsonValueKind.False)
                {
                    return value.GetBoolean();
                }

                if (value.ValueKind == JsonValueKind.String && bool.TryParse(value.GetString(), out var parsed))
                {
                    return parsed;
                }
            }
        }

        return null;
    }

    public static IReadOnlyList<string> StringList(JsonElement element, params string[] names)
    {
        foreach (var name in names)
        {
            if (!element.TryGetProperty(name, out var value))
            {
                continue;
            }

            if (value.ValueKind == JsonValueKind.String)
            {
                return value.GetString()?
                    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                    ?? Array.Empty<string>();
            }

            if (value.ValueKind == JsonValueKind.Array)
            {
                return value.EnumerateArray()
                    .Where(item => item.ValueKind == JsonValueKind.String)
                    .Select(item => item.GetString() ?? string.Empty)
                    .Where(item => item.Length > 0)
                    .ToArray();
            }
        }

        return Array.Empty<string>();
    }

    public static IReadOnlyList<AstNode> NodeList(JsonElement element, params string[] names)
    {
        foreach (var name in names)
        {
            if (element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Array)
            {
                return value.EnumerateArray().Select(AstNode.FromJson).ToArray();
            }
        }

        return Array.Empty<AstNode>();
    }

    public static IReadOnlyList<DocumentSummary> DocumentList(JsonElement element, params string[] names)
    {
        foreach (var name in names)
        {
            if (element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Array)
            {
                return value.EnumerateArray()
                    .Where(item => item.ValueKind == JsonValueKind.Object)
                    .Select(DocumentSummary.FromJson)
                    .ToArray();
            }
        }

        return Array.Empty<DocumentSummary>();
    }
}
