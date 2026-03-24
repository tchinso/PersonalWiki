(function () {
  const textarea = document.getElementById("content");
  const preview = document.getElementById("preview");
  if (!textarea || !preview) {
    return;
  }

  let timer = null;

  async function renderPreview() {
    try {
      const response = await fetch("/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content: textarea.value }),
      });
      if (!response.ok) {
        return;
      }
      const data = await response.json();
      preview.innerHTML = data.html;
    } catch (_error) {
      // Ignore network errors to keep editor responsive.
    }
  }

  textarea.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(renderPreview, 200);
  });

  renderPreview();
})();
