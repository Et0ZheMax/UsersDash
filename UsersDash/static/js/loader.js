(function () {
    "use strict";

    const SHOW_DELAY_MS = 120;
    const LOADER_ID = "globalLoader";
    let loaderEl = null;
    let active = 0;
    let showTimer = null;

    function getLoader() {
        if (loaderEl && loaderEl.isConnected) return loaderEl;
        loaderEl = document.getElementById(LOADER_ID);
        return loaderEl;
    }

    function reallyShow() {
        const el = getLoader();
        if (el && el.classList) {
            el.classList.add("show");
        }
    }

    function show() {
        active += 1;
        if (active === 1) {
            clearTimeout(showTimer);
            showTimer = window.setTimeout(reallyShow, SHOW_DELAY_MS);
        }
    }

    function hide() {
        active = Math.max(0, active - 1);
        if (active === 0) {
            clearTimeout(showTimer);
            showTimer = null;
            const el = getLoader();
            if (el && el.classList) {
                el.classList.remove("show");
            }
        }
    }

    window.EZFLoader = window.EZFLoader || {};
    window.EZFLoader.show = show;
    window.EZFLoader.hide = hide;

    const nativeFetch = window.fetch ? window.fetch.bind(window) : null;
    if (!nativeFetch || window.__fetchWithLoader) {
        return;
    }

    window.__fetchWithLoader = true;
    window.__originalFetch = nativeFetch;

    window.fetch = async function (input, init = {}) {
        const headers = new Headers(init.headers || {});
        const skip = headers.get("x-skip-loader") === "1";
        if (!skip) show();
        try {
            init.headers = headers;
            const res = await nativeFetch(input, init);
            return res;
        } finally {
            if (!skip) hide();
        }
    };
})();
