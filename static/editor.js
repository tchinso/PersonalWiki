(function () {
  const textarea = document.getElementById("content");
  const preview = document.getElementById("preview");
  const titleInput = document.getElementById("title");
  const tagsInput = document.getElementById("tags");
  const suggestionWrap = document.getElementById("tag-suggestions");
  const suggestButton = document.getElementById("suggest-tags-btn");
  const editForm = document.querySelector(".edit-form");
  const currentSlug = editForm ? editForm.dataset.currentSlug || "" : "";

  let previewTimer = null;
  let initialSnapshot = "";
  let isSubmitting = false;

  function buildSnapshot() {
    return JSON.stringify({
      title: titleInput ? titleInput.value : "",
      tags: tagsInput ? tagsInput.value : "",
      content: textarea ? textarea.value : "",
    });
  }

  function hasUnsavedChanges() {
    if (!editForm || isSubmitting) {
      return false;
    }
    return buildSnapshot() !== initialSnapshot;
  }

  function parseTags(raw) {
    const items = raw
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    const seen = new Set();
    const result = [];
    for (const tag of items) {
      const key = tag.toLowerCase();
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      result.push(tag);
    }
    return result;
  }

  function currentTagSet() {
    return new Set(parseTags(tagsInput ? tagsInput.value : "").map((tag) => tag.toLowerCase()));
  }

  function addTag(tag) {
    if (!tagsInput) {
      return;
    }
    const tags = parseTags(tagsInput.value);
    if (!tags.some((item) => item.toLowerCase() === tag.toLowerCase())) {
      tags.push(tag);
      tagsInput.value = tags.join(", ");
      tagsInput.dispatchEvent(new Event("input", { bubbles: true }));
    }
  }

  function renderSuggestions(tags) {
    if (!suggestionWrap) {
      return;
    }
    suggestionWrap.innerHTML = "";
    const existing = currentTagSet();
    const filtered = tags.filter((tag) => !existing.has(tag.toLowerCase()));

    if (!filtered.length) {
      const empty = document.createElement("p");
      empty.className = "muted";
      empty.textContent = "추천할 태그가 없습니다.";
      suggestionWrap.appendChild(empty);
      return;
    }

    for (const tag of filtered) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "suggestion-tag";
      button.textContent = `+ ${tag}`;
      button.addEventListener("click", () => {
        addTag(tag);
        renderSuggestions(filtered);
      });
      suggestionWrap.appendChild(button);
    }
  }

  async function requestTagSuggestions() {
    if (!titleInput || !textarea || !tagsInput || !suggestionWrap) {
      return;
    }

    suggestButton && (suggestButton.disabled = true);
    try {
      const response = await fetch("/api/tag-suggestions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: titleInput.value || "",
          content: textarea.value || "",
          tags: tagsInput.value || "",
          slug: currentSlug,
        }),
      });
      if (!response.ok) {
        return;
      }
      const data = await response.json();
      const tags = Array.isArray(data.tags) ? data.tags.map((item) => String(item)) : [];
      renderSuggestions(tags);
    } catch (_error) {
      // Ignore network errors to keep editor responsive.
    } finally {
      suggestButton && (suggestButton.disabled = false);
    }
  }

  async function renderPreview() {
    if (!textarea || !preview) {
      return;
    }
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

  if (textarea && preview) {
    textarea.addEventListener("input", () => {
      clearTimeout(previewTimer);
      previewTimer = setTimeout(renderPreview, 200);
    });
    renderPreview();
  }

  if (editForm) {
    initialSnapshot = buildSnapshot();

    editForm.addEventListener("submit", () => {
      isSubmitting = true;
    });

    window.addEventListener("beforeunload", (event) => {
      if (!hasUnsavedChanges()) {
        return;
      }
      event.preventDefault();
      event.returnValue = "";
    });
  }

  if (suggestButton) {
    suggestButton.addEventListener("click", requestTagSuggestions);
  }

  if (suggestionWrap) {
    try {
      const raw = suggestionWrap.dataset.initial || "[]";
      const initial = JSON.parse(raw);
      if (Array.isArray(initial)) {
        renderSuggestions(initial.map((item) => String(item)));
      }
    } catch (_error) {
      // Ignore invalid initial payload.
    }
  }
})();
