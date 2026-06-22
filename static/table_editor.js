(function () {
  const table = document.getElementById("table-editor-grid");
  const importInput = document.getElementById("table-import");
  const importButton = document.getElementById("table-import-btn");
  const importMessage = document.getElementById("table-import-message");
  const generateButton = document.getElementById("table-generate-btn");
  const copyButton = document.getElementById("table-copy-btn");
  const result = document.getElementById("table-result");
  const resultMessage = document.getElementById("table-result-message");

  if (!table) {
    return;
  }

  let cells = [
    ["", ""],
    ["", ""],
  ];

  function setMessage(node, text, isError) {
    if (!node) {
      return;
    }
    node.textContent = text || "";
    node.classList.toggle("error-text", Boolean(isError));
  }

  function makeControlButton(label, title, className, action) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = className;
    button.textContent = label;
    button.title = title;
    button.setAttribute("aria-label", title);
    button.addEventListener("click", action);
    return button;
  }

  function renderGrid() {
    table.innerHTML = "";
    const columnCount = cells[0].length;
    table.style.minWidth = `${64 + columnCount * 210}px`;

    const colgroup = document.createElement("colgroup");
    const rowControlColumn = document.createElement("col");
    rowControlColumn.className = "table-narrow-control-column";
    colgroup.appendChild(rowControlColumn);
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
      const dataColumn = document.createElement("col");
      dataColumn.className = "table-data-column";
      colgroup.appendChild(dataColumn);
    }
    const addColumn = document.createElement("col");
    addColumn.className = "table-narrow-control-column";
    colgroup.appendChild(addColumn);
    table.appendChild(colgroup);

    const head = document.createElement("thead");
    const headRow = document.createElement("tr");
    const corner = document.createElement("th");
    corner.className = "table-control-corner";
    corner.scope = "col";
    corner.setAttribute("aria-label", "행과 열 제어");
    headRow.appendChild(corner);

    for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
      const control = document.createElement("th");
      control.scope = "col";
      control.className = "table-column-control";
      control.appendChild(
        makeControlButton("×", `${columnIndex + 1}열 삭제`, "table-delete-control", () => {
          if (cells[0].length <= 1) {
            window.alert("표에는 열이 하나 이상 있어야 합니다.");
            return;
          }
          if (!window.confirm(`${columnIndex + 1}열을 삭제할까요?`)) {
            return;
          }
          cells = cells.map((row) => row.filter((_value, index) => index !== columnIndex));
          renderGrid();
        })
      );
      headRow.appendChild(control);
    }

    const addColumnCell = document.createElement("th");
    addColumnCell.className = "table-add-column-cell";
    addColumnCell.scope = "col";
    addColumnCell.appendChild(
      makeControlButton("+", "열 추가", "table-add-control", () => {
        cells.forEach((row) => row.push(""));
        renderGrid();
      })
    );
    headRow.appendChild(addColumnCell);
    head.appendChild(headRow);
    table.appendChild(head);

    const body = document.createElement("tbody");
    cells.forEach((row, rowIndex) => {
      const tr = document.createElement("tr");
      const rowControl = document.createElement("th");
      rowControl.scope = "row";
      rowControl.className = "table-row-control";
      rowControl.appendChild(
        makeControlButton("×", `${rowIndex + 1}행 삭제`, "table-delete-control", () => {
          if (cells.length <= 1) {
            window.alert("표에는 행이 하나 이상 있어야 합니다.");
            return;
          }
          if (!window.confirm(`${rowIndex + 1}행을 삭제할까요?`)) {
            return;
          }
          cells.splice(rowIndex, 1);
          renderGrid();
        })
      );
      tr.appendChild(rowControl);

      row.forEach((value, columnIndex) => {
        const td = document.createElement("td");
        const textarea = document.createElement("textarea");
        textarea.rows = 3;
        textarea.value = value;
        textarea.setAttribute("aria-label", `${rowIndex + 1}행 ${columnIndex + 1}열`);
        textarea.addEventListener("input", () => {
          cells[rowIndex][columnIndex] = textarea.value;
        });
        td.appendChild(textarea);
        tr.appendChild(td);
      });

      const endCell = document.createElement("td");
      endCell.className = "table-control-spacer";
      endCell.setAttribute("aria-hidden", "true");
      tr.appendChild(endCell);
      body.appendChild(tr);
    });
    table.appendChild(body);

    const foot = document.createElement("tfoot");
    const footRow = document.createElement("tr");
    const addRowCell = document.createElement("th");
    addRowCell.className = "table-add-row-cell";
    addRowCell.scope = "row";
    addRowCell.appendChild(
      makeControlButton("+", "행 추가", "table-add-control", () => {
        cells.push(Array(columnCount).fill(""));
        renderGrid();
      })
    );
    footRow.appendChild(addRowCell);
    const footSpacer = document.createElement("td");
    footSpacer.colSpan = columnCount + 1;
    footSpacer.className = "table-control-spacer";
    footRow.appendChild(footSpacer);
    foot.appendChild(footRow);
    table.appendChild(foot);
  }

  function splitMarkdownRow(rawLine) {
    let line = String(rawLine || "").trim();
    if (line.startsWith("|")) {
      line = line.slice(1);
    }
    if (line.endsWith("|") && !line.endsWith("\\|")) {
      line = line.slice(0, -1);
    }

    const values = [];
    let current = "";
    for (let index = 0; index < line.length; index += 1) {
      const char = line[index];
      if (char === "\\" && line[index + 1] === "|") {
        current += "|";
        index += 1;
      } else if (char === "|") {
        values.push(current.trim());
        current = "";
      } else {
        current += char;
      }
    }
    values.push(current.trim());
    return values.map((value) => value.replace(/<br\s*\/?>/gi, "\n"));
  }

  function isSeparatorRow(row) {
    return row.length > 0 && row.every((value) => /^:?-{3,}:?$/.test(value.trim()));
  }

  function parseMarkdownTable(markdown) {
    const lines = String(markdown || "")
      .replace(/\r\n?/g, "\n")
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
    if (lines.length < 2 || lines.some((line) => !line.includes("|"))) {
      throw new Error("마크다운 표를 두 줄 이상 붙여넣어 주세요.");
    }

    const parsed = lines.map(splitMarkdownRow);
    if (parsed.length >= 2 && isSeparatorRow(parsed[1])) {
      parsed.splice(1, 1);
    }
    if (!parsed.length || !parsed[0].length) {
      throw new Error("표의 칸을 찾을 수 없습니다.");
    }

    const columnCount = Math.max(...parsed.map((row) => row.length));
    if (columnCount < 1) {
      throw new Error("표에는 열이 하나 이상 있어야 합니다.");
    }
    return parsed.map((row) => [...row, ...Array(columnCount - row.length).fill("")]);
  }

  function markdownCell(value) {
    return String(value || "")
      .replace(/\r\n?/g, "\n")
      .replace(/\|/g, "\\|")
      .replace(/\n/g, "<br>");
  }

  function generateMarkdown() {
    const lines = [];
    lines.push(`| ${cells[0].map(markdownCell).join(" | ")} |`);
    lines.push(`| ${cells[0].map(() => "---").join(" | ")} |`);
    for (let index = 1; index < cells.length; index += 1) {
      lines.push(`| ${cells[index].map(markdownCell).join(" | ")} |`);
    }
    result.value = lines.join("\n");
    setMessage(resultMessage, "마크다운 표를 생성했습니다.", false);
    return result.value;
  }

  async function copyResult() {
    const text = result.value || generateMarkdown();
    try {
      await navigator.clipboard.writeText(text);
      setMessage(resultMessage, "생성 결과를 클립보드에 복사했습니다.", false);
    } catch (_error) {
      result.focus();
      result.select();
      const copied = document.execCommand("copy");
      setMessage(resultMessage, copied ? "생성 결과를 복사했습니다." : "복사하지 못했습니다. 결과를 직접 복사해 주세요.", !copied);
    }
  }

  importButton.addEventListener("click", () => {
    try {
      cells = parseMarkdownTable(importInput.value);
      renderGrid();
      setMessage(importMessage, `${cells.length}행 × ${cells[0].length}열 표를 불러왔습니다.`, false);
    } catch (error) {
      setMessage(importMessage, error.message || "표를 불러오지 못했습니다.", true);
    }
  });
  generateButton.addEventListener("click", generateMarkdown);
  copyButton.addEventListener("click", copyResult);
  renderGrid();
})();
