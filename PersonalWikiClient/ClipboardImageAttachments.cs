using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using SkiaSharp;

namespace PersonalWikiClient;

/// <summary>
/// Owns only the desktop client's portable attachment cache.  Nothing in this
/// class can locate or mutate the PersonalWiki server's img directory.
/// </summary>
internal sealed class ClipboardImageAttachments
{
    // A 4K screenshot (3,840 x 2,160) remains untouched, while unusually
    // large clipboard bitmaps cannot create multiple full-resolution GDI+ and
    // encoder buffers during a paste.  Sixteen megapixels is still ample for
    // a high-quality wiki attachment and maps to at most a 64 MB 32bpp copy.
    private const int MaxClipboardDimension = 4096;
    private const long MaxClipboardPixels = 16L * 1024 * 1024;
    private static readonly HashSet<string> SupportedDroppedImageExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp",
    };

    private readonly string _root;
    private readonly Func<DateTime> _clock;
    private readonly Func<Bitmap, string, bool> _saveWebp;

    public ClipboardImageAttachments(
        string? root = null,
        Func<DateTime>? clock = null,
        Func<Bitmap, string, bool>? saveWebp = null)
    {
        // Date folders live directly beside the portable executable so the
        // copy instruction maps one-to-one to PersonalWiki/img/YYYYMMDD.
        _root = Path.GetFullPath(root ?? AppContext.BaseDirectory);
        _clock = clock ?? (() => DateTime.Now);
        _saveWebp = saveWebp ?? TrySaveWebp;
    }

    public string Root => _root;

    public ClientImageAttachment SaveClipboardImage(Image image)
    {
        ArgumentNullException.ThrowIfNull(image);
        ClientImageAttachment? webpTarget = null;
        ClientImageAttachment? pngTarget = null;
        try
        {
            // Copy into a bounded 32bpp buffer because the Win32 clipboard can
            // release its backing object as soon as the paste handler returns.
            // Do not clone an unbounded source bitmap first: a large screenshot
            // would otherwise briefly retain two full-resolution pixel buffers.
            using var bitmap = CreateBoundedBitmapCopy(image);
            webpTarget = CreateTarget("clipboard", ".webp");
            if (TrySaveWebpOrFallback(bitmap, webpTarget.AbsolutePath))
            {
                return webpTarget;
            }

            TryDelete(webpTarget.AbsolutePath);
            pngTarget = CreateTarget("clipboard", ".png");
            bitmap.Save(pngTarget.AbsolutePath, ImageFormat.Png);
            return pngTarget;
        }
        catch
        {
            if (webpTarget is not null)
            {
                TryDelete(webpTarget.AbsolutePath);
            }

            if (pngTarget is not null)
            {
                TryDelete(pngTarget.AbsolutePath);
            }

            throw;
        }
    }

    public ClientImageAttachment CopyDroppedImage(string sourcePath)
    {
        if (string.IsNullOrWhiteSpace(sourcePath))
        {
            throw new ArgumentException("이미지 파일 경로가 비어 있습니다.", nameof(sourcePath));
        }

        var source = Path.GetFullPath(sourcePath);
        if (!File.Exists(source))
        {
            throw new FileNotFoundException("드롭한 이미지 파일을 찾을 수 없습니다.", source);
        }

        var extension = Path.GetExtension(source);
        if (!SupportedDroppedImageExtensions.Contains(extension))
        {
            throw new ArgumentException("PNG, JPEG, GIF, BMP 또는 WebP 이미지만 첨부할 수 있습니다.", nameof(sourcePath));
        }

        // Copying preserves WebP bytes and extension; it never relies on GDI+
        // decoding support for WebP.
        var target = CreateTarget("drop", extension.ToLowerInvariant());
        try
        {
            File.Copy(source, target.AbsolutePath, overwrite: false);
            return target;
        }
        catch
        {
            TryDelete(target.AbsolutePath);
            throw;
        }
    }

    public static string ToWikiImageSyntax(ClientImageAttachment attachment) => $"![[{attachment.RelativePath.Replace('\\', '/')}]]";

    /// <summary>
    /// Only attachments whose exact generated wiki syntax remains in the final
    /// editor text need a post-save copy reminder. This intentionally ignores
    /// old date folders and paste-then-delete orphans.
    /// </summary>
    public static IReadOnlyList<ClientImageAttachment> ReferencedInContent(
        string? content,
        IEnumerable<ClientImageAttachment> attachments) => attachments
            .Where(attachment => (content ?? string.Empty).Contains(ToWikiImageSyntax(attachment), StringComparison.Ordinal))
            .Distinct()
            .ToArray();

    private ClientImageAttachment CreateTarget(string prefix, string extension)
    {
        var dateFolder = _clock().ToString("yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
        var directory = Path.Combine(_root, dateFolder);
        Directory.CreateDirectory(directory);
        var fileName = $"{prefix}-{_clock():HHmmssfff}-{Guid.NewGuid():N}{extension}";
        var absolutePath = Path.Combine(directory, fileName);
        return new ClientImageAttachment(dateFolder, fileName, Path.Combine(dateFolder, fileName), absolutePath);
    }

    internal static Size GetBoundedBitmapSize(int sourceWidth, int sourceHeight)
    {
        if (sourceWidth < 1 || sourceHeight < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(sourceWidth), "이미지 크기가 올바르지 않습니다.");
        }

        var pixelScale = Math.Sqrt(MaxClipboardPixels / ((double)sourceWidth * sourceHeight));
        var dimensionScale = MaxClipboardDimension / (double)Math.Max(sourceWidth, sourceHeight);
        var scale = Math.Min(1d, Math.Min(pixelScale, dimensionScale));
        return new Size(
            Math.Max(1, (int)Math.Floor(sourceWidth * scale)),
            Math.Max(1, (int)Math.Floor(sourceHeight * scale)));
    }

    private static Bitmap CreateBoundedBitmapCopy(Image source)
    {
        var size = GetBoundedBitmapSize(source.Width, source.Height);
        var output = new Bitmap(size.Width, size.Height, PixelFormat.Format32bppPArgb);
        try
        {
            using var graphics = Graphics.FromImage(output);
            graphics.Clear(Color.Transparent);
            graphics.CompositingMode = CompositingMode.SourceCopy;
            graphics.InterpolationMode = size.Width == source.Width && size.Height == source.Height
                ? InterpolationMode.NearestNeighbor
                : InterpolationMode.HighQualityBicubic;
            graphics.PixelOffsetMode = PixelOffsetMode.HighQuality;
            graphics.DrawImage(
                source,
                new Rectangle(0, 0, size.Width, size.Height),
                0,
                0,
                source.Width,
                source.Height,
                GraphicsUnit.Pixel);
            return output;
        }
        catch
        {
            output.Dispose();
            throw;
        }
    }

    private bool TrySaveWebpOrFallback(Bitmap bitmap, string targetPath)
    {
        try
        {
            return _saveWebp(bitmap, targetPath)
                && new FileInfo(targetPath).Length > 0;
        }
        catch (Exception)
        {
            // WebP is the preferred local format, never a requirement for
            // pasting.  Retain the image as PNG when its encoder is absent or
            // rejects a clipboard bitmap.
            return false;
        }
    }

    private static bool TrySaveWebp(Bitmap bitmap, string targetPath)
    {
        try
        {
            // A normal clipboard bitmap already has a 32-bit BGRA backing
            // buffer. Encode it in place while GDI+ keeps that buffer locked;
            // this avoids a PNG snapshot plus a second Skia decode for every
            // paste. Rare GDI+ formats keep the proven PNG bridge below.
            if (TrySaveWebpFromBitmapPixels(bitmap, targetPath))
            {
                return true;
            }

            // SkiaSharp accepts a PNG snapshot of every GDI+ bitmap format.
            // Keep this compatibility fallback for indexed/otherwise unusual
            // clipboard formats without retaining clipboard-owned memory.
            using var pngSnapshot = new MemoryStream();
            bitmap.Save(pngSnapshot, ImageFormat.Png);
            pngSnapshot.Position = 0;
            using var skBitmap = SKBitmap.Decode(pngSnapshot);
            if (skBitmap is null)
            {
                return false;
            }

            using var skImage = SKImage.FromBitmap(skBitmap);
            using var encoded = skImage.Encode(SKEncodedImageFormat.Webp, quality: 90);
            if (encoded is null || encoded.Size == 0)
            {
                return false;
            }

            File.WriteAllBytes(targetPath, encoded.ToArray());
            return true;
        }
        catch (Exception)
        {
            TryDelete(targetPath);
            return false;
        }
    }

    private static bool TrySaveWebpFromBitmapPixels(Bitmap bitmap, string targetPath)
    {
        if (!TryGetSkiaAlphaType(bitmap.PixelFormat, out var alphaType))
        {
            return false;
        }

        BitmapData? locked = null;
        try
        {
            locked = bitmap.LockBits(
                new Rectangle(0, 0, bitmap.Width, bitmap.Height),
                ImageLockMode.ReadOnly,
                bitmap.PixelFormat);
            var info = new SKImageInfo(bitmap.Width, bitmap.Height, SKColorType.Bgra8888, alphaType);
            using var image = SKImage.FromPixels(info, locked.Scan0, locked.Stride);
            if (image is null)
            {
                return false;
            }

            using var encoded = image.Encode(SKEncodedImageFormat.Webp, quality: 90);
            if (encoded is null || encoded.Size == 0)
            {
                return false;
            }

            File.WriteAllBytes(targetPath, encoded.ToArray());
            return true;
        }
        finally
        {
            if (locked is not null)
            {
                bitmap.UnlockBits(locked);
            }
        }
    }

    private static bool TryGetSkiaAlphaType(PixelFormat format, out SKAlphaType alphaType)
    {
        switch (format)
        {
            case PixelFormat.Format32bppPArgb:
                alphaType = SKAlphaType.Premul;
                return true;
            case PixelFormat.Format32bppArgb:
                alphaType = SKAlphaType.Unpremul;
                return true;
            case PixelFormat.Format32bppRgb:
                alphaType = SKAlphaType.Opaque;
                return true;
            default:
                alphaType = SKAlphaType.Unknown;
                return false;
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (IOException)
        {
            // Preserve the original write error. A failed cleanup is harmless:
            // the file is in this client's own temporary attachment folder.
        }
    }
}

internal sealed record ClientImageAttachment(
    string DateFolder,
    string FileName,
    string RelativePath,
    string AbsolutePath);
