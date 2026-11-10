(function () {
    var bar = document.getElementById("pick-form");
    var countEl = document.getElementById("pick-count");
    if (!bar || !countEl) return;

    function boxes() {
        return document.querySelectorAll("input.pick[form='pick-form']");
    }

    function sync() {
        var all = boxes();
        var selected = 0;
        for (var i = 0; i < all.length; i++) {
            if (all[i].checked) selected += 1;
        }
        countEl.textContent = String(selected);
        bar.hidden = selected === 0;
        document.body.classList.toggle("picking", selected > 0);

        var groups = document.querySelectorAll("input.pick-all");
        for (var g = 0; g < groups.length; g++) {
            var name = groups[g].getAttribute("data-pick-group");
            var total = 0;
            var checked = 0;
            for (var j = 0; j < all.length; j++) {
                if (all[j].getAttribute("data-pick-group") !== name) continue;
                total += 1;
                if (all[j].checked) checked += 1;
            }
            groups[g].checked = total > 0 && checked === total;
            groups[g].indeterminate = checked > 0 && checked < total;
        }
    }

    document.addEventListener("change", function (event) {
        var target = event.target;
        if (!target || !target.classList) return;
        if (target.classList.contains("pick-all")) {
            var name = target.getAttribute("data-pick-group");
            var all = boxes();
            for (var i = 0; i < all.length; i++) {
                if (all[i].getAttribute("data-pick-group") === name) {
                    all[i].checked = target.checked;
                }
            }
        }
        if (target.classList.contains("pick") || target.classList.contains("pick-all")) {
            sync();
        }
    });

    var clear = document.getElementById("pick-clear");
    if (clear) {
        clear.addEventListener("click", function () {
            var all = boxes();
            for (var i = 0; i < all.length; i++) all[i].checked = false;
            sync();
        });
    }

    sync();
})();
