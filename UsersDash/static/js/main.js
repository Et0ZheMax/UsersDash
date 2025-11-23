// static/js/main.js
// Общий JS для UsersDash:
// - спиннеры на кнопках
// - тосты (зелёный / красный / инфо)
// - AJAX для тоггла шагов (IsActive)
// - AJAX для обновления ресурсов по ферме

(function () {
    "use strict";

    function findBtnTextEl(btn) {
        return btn.querySelector(".btn-text") || btn;
    }

    function findBtnSpinnerEl(btn) {
        return btn.querySelector(".btn-spinner");
    }

    function setButtonLoading(btn, isLoading) {
        if (!btn) return;
        const textEl = findBtnTextEl(btn);
        const spinnerEl = findBtnSpinnerEl(btn);

        if (isLoading) {
            btn.disabled = true;
            if (spinnerEl) spinnerEl.hidden = false;
            if (textEl) textEl.dataset.originalText = textEl.textContent;
        } else {
            btn.disabled = false;
            if (spinnerEl) spinnerEl.hidden = true;
            if (textEl && textEl.dataset.originalText) {
                textEl.textContent = textEl.dataset.originalText;
                delete textEl.dataset.originalText;
            }
        }
    }

    function ensureToastContainer() {
        let container = document.getElementById("toast-container");
        if (!container) {
            container = document.createElement("div");
            container.id = "toast-container";
            container.style.position = "fixed";
            container.style.top = "1rem";
            container.style.right = "1rem";
            container.style.zIndex = "9999";
            document.body.appendChild(container);
        }
        return container;
    }

    function showToast(message, type) {
        const container = ensureToastContainer();
        const toast = document.createElement("div");
        toast.className = "toast";

        toast.style.marginBottom = "0.5rem";
        toast.style.padding = "0.75rem 1rem";
        toast.style.borderRadius = "4px";
        toast.style.color = "#fff";
        toast.style.fontSize = "0.9rem";
        toast.style.boxShadow = "0 2px 6px rgba(0,0,0,0.2)";

        if (type === "success") {
            toast.style.backgroundColor = "#28a745";
        } else if (type === "error") {
            toast.style.backgroundColor = "#dc3545";
        } else {
            toast.style.backgroundColor = "#6c757d";
        }

        toast.textContent = message || "";

        container.appendChild(toast);

        setTimeout(function () {
            toast.style.opacity = "0";
            toast.style.transition = "opacity 0.3s ease";
            setTimeout(function () {
                if (toast.parentNode) {
                    toast.parentNode.removeChild(toast);
                }
            }, 300);
        }, 3500);
    }

    function autoHideFlashMessages() {
        const container = document.querySelector(".flash-container");
        if (!container) return;

        const toasts = Array.from(container.querySelectorAll(".toast"));
        toasts.forEach((toast) => {
            toast.style.transition = "opacity 0.3s ease";
            setTimeout(() => {
                toast.style.opacity = "0";
                setTimeout(() => {
                    toast.remove();
                    if (!container.querySelector(".toast")) {
                        container.remove();
                    }
                }, 300);
            }, 1500);
        });
    }

    function setupNavToggle() {
        const navs = Array.from(document.querySelectorAll('[data-role="primary-nav"]'));
        const toggleBtn = document.querySelector('[data-action="toggle-nav"]');

        if (!toggleBtn || navs.length === 0) return;

        const setExpanded = (isOpen) => {
            navs.forEach((nav) => nav.classList.toggle("is-open", isOpen));
            toggleBtn.setAttribute("aria-expanded", String(isOpen));
            toggleBtn.setAttribute("aria-label", isOpen ? "Закрыть меню" : "Открыть меню");
        };

        toggleBtn.addEventListener("click", () => {
            const isOpen = toggleBtn.getAttribute("aria-expanded") === "true";
            setExpanded(!isOpen);
        });

        navs.forEach((nav) => {
            nav.addEventListener("click", (evt) => {
                if (evt.target.closest("a")) {
                    setExpanded(false);
                }
            });
        });
    }

    function setupAccountSearch() {
        const searchInput = document.querySelector('[data-role="account-search"]');
        const rows = Array.from(document.querySelectorAll('[data-account-row]'));
        const emptyRow = document.querySelector('[data-role="account-search-empty"]');

        if (!searchInput || rows.length === 0) return;

        const normalize = (str) => (str || "").toLowerCase().replace(/\s+/g, " ").trim();

        const applyFilter = () => {
            const term = normalize(searchInput.value);
            let visibleCount = 0;

            rows.forEach((row) => {
                const haystack = normalize(row.dataset.searchText || row.textContent);
                const matches = !term || haystack.includes(term);
                row.hidden = Boolean(term) && !matches;
                if (matches) {
                    visibleCount += 1;
                }
            });

            if (emptyRow) {
                emptyRow.hidden = visibleCount > 0;
            }
        };

        searchInput.addEventListener("input", applyFilter);
        applyFilter();
    }

    // ---------- Действия ----------

    async function handleRefreshAccount(btn) {
        const accountId = btn.dataset.accountId;
        if (!accountId) {
            showToast("Не удалось определить ферму.", "error");
            return;
        }

        const row = btn.closest("tr");
        const resourcesCell = row ? row.querySelector('[data-role="resources"]') : null;
        const gainCell = row ? row.querySelector('[data-role="today-gain"]') : null;
        const updatedCell = row ? row.querySelector('[data-role="last-updated"]') : null;

        setButtonLoading(btn, true);
        const textEl = findBtnTextEl(btn);
        if (textEl) {
            textEl.textContent = "Обновляем...";
        }

        try {
            const resp = await fetch(`/account/${accountId}/refresh`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                }
            });

            let data = {};
            try {
                data = await resp.json();
            } catch (e) {
                // no-op
            }

            if (!resp.ok || !data.ok) {
                const msg = (data && (data.error || data.message)) || "Не удалось обновить ресурсы.";
                throw new Error(msg);
            }

            // Обновляем ячейки таблицы
            if (resourcesCell && data.resources_brief !== undefined && data.resources_brief !== null) {
                resourcesCell.innerHTML = data.resources_brief;
            }
            if (gainCell) {
                gainCell.textContent = data.today_gain || "—";
            }
            if (updatedCell) {
                updatedCell.textContent = data.last_updated || "—";
            }

            showToast("Ресурсы обновлены.", "success");
        } catch (err) {
            console.error(err);
            showToast(err.message || "Ошибка при обновлении ресурсов.", "error");
        } finally {
            setButtonLoading(btn, false);
        }
    }

    async function handleToggleStep(btn) {
        const accountId = btn.dataset.accountId;
        const stepIdx = btn.dataset.stepIdx;
        const currentActive = btn.dataset.currentActive === "1";
        const nextActive = !currentActive;

        if (!accountId || stepIdx === undefined) {
            showToast("Некорректные данные кнопки.", "error");
            return;
        }

        const row = btn.closest("tr");
        const statusCell = row ? row.querySelector('[data-role="step-status"]') : null;

        setButtonLoading(btn, true);
        const textEl = findBtnTextEl(btn);
        if (textEl) {
            textEl.textContent = nextActive ? "Включаем..." : "Отключаем...";
        }

        try {
            const resp = await fetch(
                `/account/${accountId}/settings/step/${stepIdx}/toggle`,
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({ is_active: nextActive })
                }
            );

            let data = {};
            try {
                data = await resp.json();
            } catch (e) {
                // no-op
            }

            if (!resp.ok || !data.ok) {
                const msg = (data && (data.error || data.message)) || "Не удалось сохранить настройки шага.";
                throw new Error(msg);
            }

            btn.dataset.currentActive = nextActive ? "1" : "0";

            if (statusCell) {
                statusCell.textContent = nextActive ? "Включен" : "Выключен";
            }

            if (textEl) {
                textEl.textContent = nextActive ? "Отключить" : "Включить";
            }

            showToast("Настройки шага сохранены.", "success");
        } catch (err) {
            console.error(err);
            showToast(err.message || "Ошибка при обновлении шага.", "error");
        } finally {
            setButtonLoading(btn, false);
        }
    }


        function applyFarmDataStatusUI(status) {
            const table = document.querySelector('[data-role="farmdata-table"]');
            let missingCount = 0;

            if (table) {
                const rows = table.querySelectorAll("tbody tr[data-account-id]");
                rows.forEach(function (tr) {
                    const requiredFilled = ["email", "password", "igg_id", "server", "telegram_tag"].every(
                        function (name) {
                            const input = tr.querySelector(`input[name="${name}"]`);
                            return input && input.value && input.value.trim();
                        }
                    );

                    const badge = tr.querySelector(".farmdata-status-badge");
                    if (badge) {
                        badge.textContent = requiredFilled ? "Заполнено" : "Нужно заполнить";
                        badge.classList.toggle("farmdata-status-ok", requiredFilled);
                        badge.classList.toggle("farmdata-status-empty", !requiredFilled);
                    }

                    if (!requiredFilled) missingCount += 1;
                });
            }

            const hasIssues = status ? !!status.has_issues : missingCount > 0;

            const alertBox = document.querySelector(".farmdata-alert");
            if (alertBox) {
                alertBox.style.display = hasIssues ? "" : "none";
            }

            const badgeRequired = document.querySelector(".farmdata-badge-required");
            if (badgeRequired) {
                badgeRequired.style.display = hasIssues ? "inline-flex" : "none";
            }

            const navLink = document.querySelector('.nav .nav-link[href$="/farm-data"]');
            if (navLink) {
                navLink.classList.toggle("nav-link-attention", hasIssues);
            }
        }

        async function handleFarmDataSave(btn) {
            const table = document.querySelector('[data-role="farmdata-table"]');
            if (!table) {
                showToast("Таблица с фермами не найдена.", "error");
                return;
            }

            const rows = table.querySelectorAll("tbody tr[data-account-id]");
            if (!rows.length) {
                showToast("Нет ферм для сохранения.", "info");
                return;
            }

            const items = [];

            rows.forEach(function (tr) {
                const accountId = tr.dataset.accountId;
                if (!accountId) return;

                function val(selector) {
                    const el = tr.querySelector(selector);
                    return el && el.value ? el.value.trim() : "";
                }

                const item = {
                    account_id: accountId,
                    email: val('input[name="email"]'),
                    password: val('input[name="password"]'),
                    igg_id: val('input[name="igg_id"]'),
                    server: val('input[name="server"]'),
                    telegram_tag: val('input[name="telegram_tag"]')
                    // ВНИМАНИЕ: дату оплаты и тариф клиент больше не меняет,
                    // они изменяются только из админки.
                };

                items.push(item);
            });

            if (!items.length) {
                showToast("Нет данных для сохранения.", "info");
                return;
            }

            setButtonLoading(btn, true);
            try {
                const resp = await fetch("/farm-data/save", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({ items: items })
                });

                let data = {};
                try {
                    data = await resp.json();
                } catch (_) {
                    data = {};
                }

                if (!resp.ok || !data.ok) {
                    throw new Error(data.error || "Ошибка при сохранении данных.");
                }

                applyFarmDataStatusUI(data.farmdata_status);
                showToast("Данные ферм сохранены.", "success");
            } catch (err) {
                console.error(err);
                showToast(err.message || "Ошибка при сохранении данных.", "error");
            } finally {
                setButtonLoading(btn, false);
            }
        }

    document.addEventListener("DOMContentLoaded", function () {
        autoHideFlashMessages();
        setupNavToggle();
        setupAccountSearch();
    });

    // ---------- Делегированный обработчик кликов ----------

    document.addEventListener("click", function (event) {
        const btn = event.target.closest("button[data-action]");
        if (!btn) return;

        const action = btn.dataset.action;
        if (!action) return;

        if (action === "refresh-account") {
            event.preventDefault();
            handleRefreshAccount(btn);
        } else if (action === "toggle-step") {
            event.preventDefault();
            handleToggleStep(btn);
        } else if (action === "farmdata-save") {
            event.preventDefault();
            handleFarmDataSave(btn);
        }
    });

})();
