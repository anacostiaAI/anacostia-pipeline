document.addEventListener("htmx:afterSwap", (e) => {
    if (window.MathJax) {
        MathJax.typesetPromise();
    }
});
