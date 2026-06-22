(function () {
  const form = document.getElementById("package-form");
  const address = document.getElementById("document-address");
  const confirmed = document.getElementById("confirmed-files");
  const submitButton = document.getElementById("package-submit-btn");
  const message = document.getElementById("package-message");

  if (!form || !address || !confirmed) {
    return;
  }

  function selectedFormat() {
    const selected = form.querySelector('input[name="export_format"]:checked');
    return selected ? selected.value : "";
  }

  function showMessage(text, isError) {
    if (!message) {
      return;
    }
    message.textContent = text || "";
    message.classList.toggle("error-text", Boolean(isError));
  }

  address.addEventListener("input", () => {
    confirmed.value = "0";
  });
  form.querySelectorAll('input[name="export_format"]').forEach((radio) => {
    radio.addEventListener("change", () => {
      confirmed.value = "0";
    });
  });

  form.addEventListener("submit", async (event) => {
    if (selectedFormat() !== "html" || confirmed.value === "1") {
      return;
    }
    event.preventDefault();
    submitButton.disabled = true;
    showMessage("문서의 첨부 파일을 확인하고 있습니다.", false);

    try {
      const response = await fetch("/api/package/check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ document_address: address.value }),
      });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        showMessage(data.error || "문서를 확인하지 못했습니다.", true);
        return;
      }

      if (data.has_files) {
        const proceed = window.confirm(
          `이 문서에서 첨부 파일 링크 ${data.file_count}개를 찾았습니다.\n` +
          "첨부 파일은 단일 HTML 안에 포함되지 않아 링크가 작동하지 않을 수 있습니다. 계속할까요?"
        );
        if (!proceed) {
          showMessage("단일 HTML 내보내기를 취소했습니다.", false);
          return;
        }
      }

      confirmed.value = "1";
      showMessage("내보내기 파일을 만들고 있습니다.", false);
      form.submit();
    } catch (_error) {
      showMessage("문서를 확인하는 중 네트워크 오류가 발생했습니다.", true);
    } finally {
      submitButton.disabled = false;
    }
  });
})();
