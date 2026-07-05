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

  let rows = [
    { cells: ["", ""], widths: [3, 3] },
    { cells: ["", ""], widths: [3, 3] },
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

  function normalizeWidth(value) {
    const parsed = Number.parseInt(String(value), 10);
    if (!Number.isFinite(parsed)) {
      return 3;
    }
    return Math.max(3, Math.min(24, parsed));
  }

  function defaultWidths(count) {
    return Array(Math.max(1, count)).fill(3);
  }

  function normalizeRow(row) {
    const cells = Array.isArray(row.cells) && row.cells.length ? row.cells.map((value) => String(value || "")) : [""];
    const sourceWidths = Array.isArray(row.widths) ? row.widths : [];
    const widths = cells.map((_cell, index) => normalizeWidth(sourceWidths[index] || 3));
    return { cells, widths };
  }

  function maxColumnCount() {
    return Math.max(1, ...rows.map((row) => row.cells.length));
  }

  function sameWidths(left, right) {
    return left.length === right.length && left.every((value, index) => value === right[index]);
  }

  function renderGrid() {
    rows = rows.map(normalizeRow);
    table.innerHTML = "";
    const columnCount = maxColumnCount();
    table.style.minWidth = `${96 + columnCount * 230}px`;

    const colgroup = document.createElement("colgroup");
    const rowControlColumn = document.createElement("col");
    rowControlColumn.className = "table-narrow-control-column";
    colgroup.appendChild(rowControlColumn);
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
      const dataColumn = document.createElement("col");
      dataColumn.className = "table-data-column";
      colgroup.appendChild(dataColumn);
    }
    const rowActionColumn = document.createElement("col");
    rowActionColumn.className = "table-narrow-control-column";
    colgroup.appendChild(rowActionColumn);
    table.appendChild(colgroup);

    const head = document.createElement("thead");
    const headRow = document.createElement("tr");
    const corner = document.createElement("th");
    corner.className = "table-control-corner";
    corner.scope = "col";
    corner.setAttribute("aria-label", "행과 열 제어");
    headRow.appendChild(corner);

    for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
      const label = document.createElement("th");
      label.scope = "col";
      label.className = "table-column-label";
      label.textContent = `${columnIndex + 1}열`;
      headRow.appendChild(label);
    }

    const actionHeader = document.createElement("th");
    actionHeader.scope = "col";
    actionHeader.className = "table-control-corner";
    actionHeader.textContent = "열";
    headRow.appendChild(actionHeader);
    head.appendChild(headRow);
    table.appendChild(head);

    const body = document.createElement("tbody");
    rows.forEach((row, rowIndex) => {
      const tr = document.createElement("tr");
      const rowControl = document.createElement("th");
      rowControl.scope = "row";
      rowControl.className = "table-row-control";

      const rowLabel = document.createElement("span");
      rowLabel.className = "table-row-label";
      rowLabel.textContent = rowIndex === 0 ? "제목" : `${rowIndex + 1}행`;
      rowControl.appendChild(rowLabel);
      rowControl.appendChild(
        makeControlButton("×", `${rowIndex + 1}행 삭제`, "table-delete-control", () => {
          if (rows.length <= 1) {
            window.alert("표에는 행이 하나 이상 있어야 합니다.");
            return;
          }
          if (!window.confirm(`${rowIndex + 1}행을 삭제할까요?`)) {
            return;
          }
          rows.splice(rowIndex, 1);
          renderGrid();
        })
      );
      tr.appendChild(rowControl);

      for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
        const td = document.createElement("td");
        if (columnIndex >= row.cells.length) {
          td.className = "table-empty-cell";
          td.setAttribute("aria-hidden", "true");
          tr.appendChild(td);
          continue;
        }

        const tools = document.createElement("div");
        tools.className = "table-cell-tools";
        tools.appendChild(
          makeControlButton("×", `${rowIndex + 1}행 ${columnIndex + 1}열 삭제`, "table-delete-control", () => {
            if (row.cells.length <= 1) {
              window.alert("각 행에는 열이 하나 이상 있어야 합니다.");
              return;
            }
            row.cells.splice(columnIndex, 1);
            row.widths.splice(columnIndex, 1);
            renderGrid();
          })
        );

        const widthLabel = document.createElement("label");
        widthLabel.className = "table-width-control";
        widthLabel.textContent = "비율";
        const widthInput = document.createElement("input");
        widthInput.className = "table-width-input";
        widthInput.type = "number";
        widthInput.min = "3";
        widthInput.max = "24";
        widthInput.step = "1";
        widthInput.value = String(row.widths[columnIndex]);
        widthInput.setAttribute("aria-label", `${rowIndex + 1}행 ${columnIndex + 1}열 구분선 길이`);
        widthInput.addEventListener("input", () => {
          row.widths[columnIndex] = normalizeWidth(widthInput.value);
        });
        widthInput.addEventListener("change", () => {
          widthInput.value = String(normalizeWidth(widthInput.value));
          row.widths[columnIndex] = normalizeWidth(widthInput.value);
        });
        widthLabel.appendChild(widthInput);
        tools.appendChild(widthLabel);
        td.appendChild(tools);

        const textarea = document.createElement("textarea");
        textarea.rows = 3;
        textarea.value = row.cells[columnIndex];
        textarea.setAttribute("aria-label", `${rowIndex + 1}행 ${columnIndex + 1}열`);
        textarea.addEventListener("input", () => {
          row.cells[columnIndex] = textarea.value;
        });
        td.appendChild(textarea);
        tr.appendChild(td);
      }

      const rowAction = document.createElement("td");
      rowAction.className = "table-row-action";
      rowAction.appendChild(
        makeControlButton("+", `${rowIndex + 1}행에 열 추가`, "table-add-control", () => {
          row.cells.push("");
          row.widths.push(3);
          renderGrid();
        })
      );
      tr.appendChild(rowAction);
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
        const source = rows[rows.length - 1] || { cells: [""], widths: [3] };
        rows.push({
          cells: Array(source.cells.length).fill(""),
          widths: [...source.widths],
        });
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

  function separatorWidths(row) {
    if (!row.length) {
      return null;
    }
    const widths = [];
    for (const value of row) {
      const match = /^:?(---+):?$/.exec(value.trim());
      if (!match) {
        return null;
      }
      widths.push(match[1].length);
    }
    return widths;
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
    const firstWidths = separatorWidths(parsed[1]);
    if (!firstWidths) {
      throw new Error("두 번째 줄에는 --- 구분 행이 있어야 합니다.");
    }
    if (parsed[0].length !== firstWidths.length) {
      throw new Error("제목 행과 첫 구분 행의 열 수가 다릅니다.");
    }

    let activeWidths = firstWidths;
    const parsedRows = [{ cells: parsed[0], widths: [...firstWidths] }];
    for (let index = 2; index < parsed.length; index += 1) {
      const widths = separatorWidths(parsed[index]);
      if (widths) {
        activeWidths = widths;
        continue;
      }

      const cells = parsed[index];
      parsedRows.push({
        cells,
        widths: activeWidths.length === cells.length ? [...activeWidths] : defaultWidths(cells.length),
      });
    }

    if (!parsedRows.length || !parsedRows[0].cells.length) {
      throw new Error("표의 칸을 찾을 수 없습니다.");
    }
    return parsedRows.map(normalizeRow);
  }

  function markdownCell(value) {
    return String(value || "")
      .replace(/\r\n?/g, "\n")
      .replace(/\|/g, "\\|")
      .replace(/\n/g, "<br>");
  }

  function separatorRow(widths) {
    return `| ${widths.map((width) => "-".repeat(normalizeWidth(width))).join(" | ")} |`;
  }

  function contentRow(cells) {
    return `| ${cells.map(markdownCell).join(" | ")} |`;
  }

  function generateMarkdown() {
    rows = rows.map(normalizeRow);
    const lines = [];
    lines.push(contentRow(rows[0].cells));
    lines.push(separatorRow(rows[0].widths));

    let activeWidths = [...rows[0].widths];
    for (let index = 1; index < rows.length; index += 1) {
      if (!sameWidths(activeWidths, rows[index].widths)) {
        lines.push(separatorRow(rows[index].widths));
        activeWidths = [...rows[index].widths];
      }
      lines.push(contentRow(rows[index].cells));
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
      rows = parseMarkdownTable(importInput.value);
      renderGrid();
      const layouts = new Set(rows.map((row) => row.widths.join(":"))).size;
      setMessage(importMessage, `${rows.length}행, 최대 ${maxColumnCount()}열, ${layouts}개 레이아웃을 불러왔습니다.`, false);
    } catch (error) {
      setMessage(importMessage, error.message || "표를 불러오지 못했습니다.", true);
    }
  });
  generateButton.addEventListener("click", generateMarkdown);
  copyButton.addEventListener("click", copyResult);
  renderGrid();
})();
