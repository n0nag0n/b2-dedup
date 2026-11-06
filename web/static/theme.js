(function () {
    var key = "si-theme";

    function systemTheme() {
        return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }

    function chosen() {
        try {
            var stored = localStorage.getItem(key);
            return stored === "light" || stored === "dark" ? stored : null;
        } catch (e) {
            return null;
        }
    }

    function current() {
        var attr = document.documentElement.getAttribute("data-theme");
        return attr === "light" || attr === "dark" ? attr : systemTheme();
    }

    function paint(theme) {
        document.documentElement.setAttribute("data-theme", theme);
        var button = document.getElementById("theme-toggle");
        if (!button) return;
        var dark = theme === "dark";
        button.setAttribute("aria-pressed", dark ? "true" : "false");
        button.setAttribute("aria-label", dark ? "Switch to light mode" : "Switch to dark mode");
    }

    paint(current());

    var button = document.getElementById("theme-toggle");
    if (button) {
        button.addEventListener("click", function () {
            var next = current() === "dark" ? "light" : "dark";
            try { localStorage.setItem(key, next); } catch (e) {}
            paint(next);
        });
    }

    var query = window.matchMedia("(prefers-color-scheme: dark)");
    function followSystem() {
        if (!chosen()) paint(systemTheme());
    }
    if (query.addEventListener) query.addEventListener("change", followSystem);
    else if (query.addListener) query.addListener(followSystem);
})();
