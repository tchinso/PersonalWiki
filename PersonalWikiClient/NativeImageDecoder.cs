using SkiaSharp;
using System.Drawing.Imaging;
using System.Runtime.InteropServices;

namespace PersonalWikiClient;

/// <summary>
/// Decodes image bytes received through the localhost API or an allowed remote
/// image URL.  GDI+ remains the fast path for the formats Windows supports.
/// WebP is decoded through the bundled SkiaSharp native codec because GDI+
/// does not consistently include a WebP codec on Windows.
/// </summary>
internal static class NativeImageDecoder
{
    // A displayed PictureBox is at most 960 px wide. Decoding a camera-sized
    // source at its full resolution would spend tens or hundreds of MB only to
    // scale it down immediately. Keep enough pixels for sharp high-DPI output
    // while bounding malformed/compressed image expansion.
    private const int MaxDecodedDimension = 2048;
    private const long MaxDecodedPixels = 4L * 1024 * 1024;

    public static Image? Decode(byte[] bytes)
    {
        if (bytes.Length == 0)
        {
            return null;
        }

        // Send WebP through the bundled decoder first.  This avoids relying on
        // an optional OS codec and makes file-drop WebP attachments render the
        // same way on every supported Windows installation.
        if (LooksLikeWebp(bytes))
        {
            return DecodeWithSkiaSharp(bytes) ?? DecodeWithGdiPlus(bytes);
        }

        return DecodeWithGdiPlus(bytes) ?? DecodeWithSkiaSharp(bytes);
    }

    private static Image? DecodeWithGdiPlus(byte[] bytes)
    {
        try
        {
            using var stream = new MemoryStream(bytes, writable: false);
            // Header parsing exposes dimensions before cloning into a native
            // bitmap. Large sources then take the bounded Skia path below.
            using var image = Image.FromStream(stream, useEmbeddedColorManagement: false, validateImageData: false);
            if (NeedsBoundedDecode(image.Width, image.Height))
            {
                return null;
            }

            return new Bitmap(image);
        }
        catch (Exception error) when (error is ArgumentException or ExternalException or OutOfMemoryException)
        {
            return null;
        }
    }

    private static Image? DecodeWithSkiaSharp(byte[] bytes)
    {
        try
        {
            // SKCodec reads from this managed stream without the additional
            // SKData.CreateCopy allocation. It also decodes directly into the
            // GDI+ bitmap owned by the PictureBox, avoiding a WebP -> PNG ->
            // GDI+ round-trip and its large temporary buffers.
            using var stream = new MemoryStream(bytes, writable: false);
            using var codec = SKCodec.Create(stream);
            if (codec is null || codec.Info.Width < 1 || codec.Info.Height < 1)
            {
                return null;
            }

            var dimensions = GetBoundedDimensions(codec);
            if (dimensions.Width < 1 || dimensions.Height < 1
                || NeedsBoundedDecode(dimensions.Width, dimensions.Height))
            {
                return null;
            }

            var info = codec.Info
                .WithSize(dimensions)
                .WithColorType(SKColorType.Bgra8888)
                .WithAlphaType(SKAlphaType.Premul);
            var output = new Bitmap(info.Width, info.Height, PixelFormat.Format32bppPArgb);
            BitmapData? locked = null;
            var decoded = false;
            try
            {
                locked = output.LockBits(
                    new Rectangle(0, 0, output.Width, output.Height),
                    ImageLockMode.WriteOnly,
                    PixelFormat.Format32bppPArgb);
                decoded = codec.GetPixels(info, locked.Scan0, locked.Stride, new SKCodecOptions()) == SKCodecResult.Success;
                return decoded ? output : null;
            }
            finally
            {
                if (locked is not null)
                {
                    output.UnlockBits(locked);
                }

                if (!decoded)
                {
                    output.Dispose();
                }
            }
        }
        catch (ArgumentException)
        {
            return null;
        }
        // The native codec can report several platform-specific exception
        // types (including a missing/corrupt native payload).  An image
        // failure must remain isolated from the document renderer.
        catch (Exception)
        {
            return null;
        }
    }

    private static bool LooksLikeWebp(ReadOnlySpan<byte> bytes) => bytes.Length >= 12
        && bytes[..4].SequenceEqual("RIFF"u8)
        && bytes.Slice(8, 4).SequenceEqual("WEBP"u8);

    private static SKSizeI GetBoundedDimensions(SKCodec codec)
    {
        var source = codec.Info;
        var pixelScale = Math.Sqrt(MaxDecodedPixels / ((double)source.Width * source.Height));
        var dimensionScale = MaxDecodedDimension / (double)Math.Max(source.Width, source.Height);
        var scale = (float)Math.Min(1d, Math.Min(pixelScale, dimensionScale));
        return codec.GetScaledDimensions(scale);
    }

    private static bool NeedsBoundedDecode(int width, int height) => width > MaxDecodedDimension
        || height > MaxDecodedDimension
        || (long)width * height > MaxDecodedPixels;
}
