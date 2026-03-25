(function () {
  function toggleSpoiler(node) {
    if (!node) {
      return;
    }
    const revealed = node.classList.toggle("revealed");
    node.setAttribute("aria-pressed", revealed ? "true" : "false");
  }

  document.addEventListener("click", (event) => {
    const target = event.target.closest(".spoiler");
    if (!target) {
      return;
    }
    toggleSpoiler(target);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    const target = event.target.closest(".spoiler");
    if (!target) {
      return;
    }
    event.preventDefault();
    toggleSpoiler(target);
  });
})();
