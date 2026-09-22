using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

namespace PersonalWikiClient;

/// <summary>
/// HTTP-only localhost client for the dedicated /api/client server contract.
/// No database, document file, server configuration, HTML page, or browser engine
/// is accessed from this assembly.
/// </summary>
internal sealed class ClientApi : IDisposable
{
    private readonly HttpClient _http;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
    };

    public ClientApi(int port)
    {
        if (port is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(port));
        }

        BaseUri = new Uri($"http://127.0.0.1:{port}/", UriKind.Absolute);
        _http = new HttpClient(new HttpClientHandler
        {
            AllowAutoRedirect = false,
            UseCookies = false,
        })
        {
            BaseAddress = BaseUri,
            Timeout = TimeSpan.FromSeconds(12),
        };
        _http.DefaultRequestHeaders.Accept.ParseAdd("application/json");
    }

    public Uri BaseUri { get; }

    public async Task<IReadOnlyList<DocumentSummary>> GetDocumentsAsync(CancellationToken cancellationToken)
    {
        // The server caps explicit pages at 1,000 records. Follow next_offset
        // rather than silently showing only the first page for a large wiki.
        const int pageSize = 1000;
        var offset = 0;
        var seenOffsets = new HashSet<int>();
        var documents = new List<DocumentSummary>();
        while (seenOffsets.Add(offset))
        {
            var path = $"api/client/documents?limit={pageSize}&offset={offset}";
            using var response = await SendAsync(HttpMethod.Get, path, null, cancellationToken);
            using var json = await ReadJsonAsync(response, cancellationToken);
            documents.AddRange(JsonValue.DocumentList(json.RootElement, "documents", "items"));
            if (!json.RootElement.TryGetProperty("pagination", out var pagination)
                || pagination.ValueKind != JsonValueKind.Object
                || JsonValue.Int(pagination, "next_offset", "nextOffset") is not int nextOffset)
            {
                return documents;
            }

            if (nextOffset <= offset)
            {
                throw new InvalidOperationException("문서 목록 페이지 정보가 올바르지 않습니다.");
            }

            offset = nextOffset;
        }

        throw new InvalidOperationException("문서 목록 페이지 정보가 반복됩니다.");
    }

    public async Task<DocumentPayload> GetDocumentAsync(string slug, CancellationToken cancellationToken)
    {
        using var response = await SendAsync(HttpMethod.Get, $"api/client/documents/{EncodePath(slug)}", null, cancellationToken);
        using var json = await ReadJsonAsync(response, cancellationToken);
        return DocumentPayload.FromJson(json.RootElement);
    }

    public async Task<IReadOnlyList<DocumentSummary>> SearchAsync(string query, CancellationToken cancellationToken)
    {
        var path = "api/client/search?q=" + Uri.EscapeDataString(query ?? string.Empty);
        using var response = await SendAsync(HttpMethod.Get, path, null, cancellationToken);
        using var json = await ReadJsonAsync(response, cancellationToken);
        return JsonValue.DocumentList(json.RootElement, "documents", "results", "items");
    }

    public async Task<IReadOnlyList<DocumentSummary>> GetTagDocumentsAsync(string tag, CancellationToken cancellationToken)
    {
        using var response = await SendAsync(HttpMethod.Get, $"api/client/tags/{Uri.EscapeDataString(tag)}", null, cancellationToken);
        using var json = await ReadJsonAsync(response, cancellationToken);
        return JsonValue.DocumentList(json.RootElement, "documents", "items");
    }

    public async Task<RenderPayload> RenderAsync(string content, CancellationToken cancellationToken)
    {
        using var request = JsonContent.Create(new { content }, options: _jsonOptions);
        using var response = await SendAsync(HttpMethod.Post, "api/client/render", request, cancellationToken);
        using var json = await ReadJsonAsync(response, cancellationToken);
        return RenderPayload.FromJson(json.RootElement);
    }

    public async Task<SaveDocumentResult> CreateDocumentAsync(SaveDocumentRequest document, CancellationToken cancellationToken)
    {
        using var request = CreateSaveContent(document);
        using var response = await SendAsync(HttpMethod.Post, "api/client/documents", request, cancellationToken, throwOnError: false);
        using var json = await ReadJsonAsync(response, cancellationToken, throwOnError: false);
        var result = SaveDocumentResult.FromJson(json.RootElement);
        if (!response.IsSuccessStatusCode && string.IsNullOrWhiteSpace(result.Error))
        {
            throw ClientApiException.FromResponse(response.StatusCode, json.RootElement);
        }

        return result;
    }

    public async Task<SaveDocumentResult> UpdateDocumentAsync(string slug, SaveDocumentRequest document, CancellationToken cancellationToken)
    {
        using var request = CreateSaveContent(document);
        using var response = await SendAsync(HttpMethod.Put, $"api/client/documents/{EncodePath(slug)}", request, cancellationToken, throwOnError: false);
        using var json = await ReadJsonAsync(response, cancellationToken, throwOnError: false);
        var result = SaveDocumentResult.FromJson(json.RootElement);
        if (!response.IsSuccessStatusCode && string.IsNullOrWhiteSpace(result.Error))
        {
            throw ClientApiException.FromResponse(response.StatusCode, json.RootElement);
        }

        return result;
    }

    public async Task DeleteDocumentAsync(string slug, CancellationToken cancellationToken)
    {
        using var response = await SendAsync(HttpMethod.Delete, $"api/client/documents/{EncodePath(slug)}", null, cancellationToken);
        _ = await ReadJsonAsync(response, cancellationToken);
    }

    public async Task<IReadOnlyList<string>> GetTagSuggestionsAsync(
        string title,
        string content,
        string? currentSlug,
        IReadOnlyList<string> tags,
        CancellationToken cancellationToken)
    {
        using var request = JsonContent.Create(
            new { title, content, slug = currentSlug, tags },
            options: _jsonOptions);
        using var response = await SendAsync(HttpMethod.Post, "api/client/tag-suggestions", request, cancellationToken);
        using var json = await ReadJsonAsync(response, cancellationToken);
        return JsonValue.StringList(json.RootElement, "tags", "suggestions", "recommended_tags");
    }

    public async Task<byte[]> GetAssetBytesAsync(string relativePath, CancellationToken cancellationToken)
    {
        return await GetAssetBytesAsync(relativePath, maximumBytes: null, cancellationToken);
    }

    /// <summary>
    /// Reads an image asset without allowing a malformed or unexpectedly huge
    /// localhost response to allocate an unbounded byte array in the client.
    /// Downloads intentionally use <see cref="GetAssetBytesAsync(string, CancellationToken)"/>
    /// instead, because they are explicitly chosen by the user.
    /// </summary>
    public async Task<byte[]> GetAssetBytesUpToAsync(
        string relativePath,
        int maximumBytes,
        CancellationToken cancellationToken)
    {
        if (maximumBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumBytes));
        }

        return await GetAssetBytesAsync(relativePath, maximumBytes, cancellationToken);
    }

    private async Task<byte[]> GetAssetBytesAsync(
        string relativePath,
        int? maximumBytes,
        CancellationToken cancellationToken)
    {
        var safePath = EncodePath(relativePath);
        using var response = await SendAsync(HttpMethod.Get, safePath, null, cancellationToken);
        if (maximumBytes is int maximum
            && response.Content.Headers.ContentLength is long contentLength
            && contentLength > maximum)
        {
            throw new InvalidOperationException($"이미지 파일이 {maximum / 1024 / 1024}MB 제한을 초과했습니다.");
        }

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var copied = new MemoryStream();
        var buffer = new byte[81920];
        var total = 0;
        while (true)
        {
            var count = await stream.ReadAsync(buffer, cancellationToken);
            if (count == 0)
            {
                break;
            }

            checked
            {
                total += count;
            }
            if (maximumBytes is int limit && total > limit)
            {
                throw new InvalidOperationException($"이미지 파일이 {limit / 1024 / 1024}MB 제한을 초과했습니다.");
            }

            await copied.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
        }

        return copied.ToArray();
    }

    public Uri GetLocalAssetUri(string relativePath) => new(BaseUri, EncodePath(relativePath));

    private JsonContent CreateSaveContent(SaveDocumentRequest document) => JsonContent.Create(
        new
        {
            title = document.Title,
            content = document.Content,
            tags = document.Tags,
            save_as_is = document.SaveAsIs,
            ignore_tag_warning = document.IgnoreTagWarning,
        },
        options: _jsonOptions);

    private async Task<HttpResponseMessage> SendAsync(
        HttpMethod method,
        string path,
        HttpContent? content,
        CancellationToken cancellationToken,
        bool throwOnError = true)
    {
        using var request = new HttpRequestMessage(method, path) { Content = content };
        var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (!throwOnError || response.IsSuccessStatusCode)
        {
            return response;
        }

        using (response)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            throw ClientApiException.FromResponse(response.StatusCode, body);
        }
    }

    private static async Task<JsonDocument> ReadJsonAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken,
        bool throwOnError = true)
    {
        // Success payloads include full document lists and ASTs. Parsing the
        // response stream avoids first materializing an equally large UTF-16
        // string, which materially reduces transient memory for big wikis.
        if (response.IsSuccessStatusCode)
        {
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        }

        // Error bodies are normally small and need a text fallback when a
        // proxy or an older server returns non-JSON. Keep that compatibility
        // path while leaving normal reads streaming.
        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        try
        {
            var json = JsonDocument.Parse(content);
            if (throwOnError)
            {
                using (json)
                {
                    throw ClientApiException.FromResponse(response.StatusCode, json.RootElement);
                }
            }

            return json;
        }
        catch (JsonException) when (!response.IsSuccessStatusCode)
        {
            throw ClientApiException.FromResponse(response.StatusCode, content);
        }
    }

    private static string EncodePath(string value)
    {
        var rawSegments = (value ?? string.Empty)
            .Replace('\\', '/')
            .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (rawSegments.Length == 0)
        {
            throw new ArgumentException("안전하지 않은 서버 경로입니다.", nameof(value));
        }

        var segments = new List<string>(rawSegments.Length);
        foreach (var rawSegment in rawSegments)
        {
            string segment;
            try
            {
                segment = Uri.UnescapeDataString(rawSegment);
            }
            catch (UriFormatException error)
            {
                throw new ArgumentException("안전하지 않은 서버 경로입니다.", nameof(value), error);
            }

            if (segment.Length == 0
                || segment is "." or ".."
                || segment.IndexOfAny(['/', '\\', '\0']) >= 0
                || segment.Any(char.IsControl))
            {
                throw new ArgumentException("안전하지 않은 서버 경로입니다.", nameof(value));
            }

            segments.Add(Uri.EscapeDataString(segment));
        }

        return string.Join('/', segments);
    }

    public void Dispose() => _http.Dispose();
}

internal sealed class ClientApiException : Exception
{
    public ClientApiException(HttpStatusCode statusCode, string message)
        : base(message)
    {
        StatusCode = statusCode;
    }

    public HttpStatusCode StatusCode { get; }

    public static ClientApiException FromResponse(HttpStatusCode statusCode, JsonElement root) =>
        new(statusCode, JsonValue.String(root, "error", "message", "detail")
            ?? $"서버 요청이 실패했습니다. (HTTP {(int)statusCode})");

    public static ClientApiException FromResponse(HttpStatusCode statusCode, string body)
    {
        try
        {
            using var json = JsonDocument.Parse(body);
            return FromResponse(statusCode, json.RootElement);
        }
        catch (JsonException)
        {
            return new ClientApiException(
                statusCode,
                $"서버 요청이 실패했습니다. (HTTP {(int)statusCode})");
        }
    }
}
