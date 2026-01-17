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

    async function loadAdminAccountResources() {
        const table = document.querySelector('[data-role="accounts-table"]');
        if (!table) return;

        const endpoint = table.dataset.resourcesEndpoint;
        if (!endpoint) return;

        const rows = new Map();
        table.querySelectorAll('tbody tr[data-account-id]').forEach((row) => {
            const accountId = row.dataset.accountId;
            if (!accountId) return;

            rows.set(accountId, {
                resourcesCell: row.querySelector('[data-role="resources"]'),
                gainCell: row.querySelector('[data-role="today-gain"]'),
                updatedCell: row.querySelector('[data-role="last-updated"]'),
            });
        });

        if (rows.size === 0) return;

        try {
            const resp = await fetch(endpoint, { headers: { 'x-skip-loader': '1' } });
            const data = await resp.json().catch(() => ({}));

            if (!resp.ok || !data.ok) {
                throw new Error((data && data.error) || 'Не удалось загрузить ресурсы.');
            }

            (data.items || []).forEach((item) => {
                const rowInfo = rows.get(String(item.account_id));
                if (!rowInfo) return;

                const resourcesValue = item.resources_brief || '—';
                const gainValue = item.today_gain || '—';
                const updatedValue = item.last_updated || '—';

                if (rowInfo.resourcesCell) {
                    rowInfo.resourcesCell.innerHTML = resourcesValue;
                    rowInfo.resourcesCell.dataset.state = 'loaded';
                }
                if (rowInfo.gainCell) {
                    rowInfo.gainCell.textContent = gainValue;
                    rowInfo.gainCell.dataset.state = 'loaded';
                }
                if (rowInfo.updatedCell) {
                    rowInfo.updatedCell.textContent = updatedValue;
                    rowInfo.updatedCell.dataset.state = 'loaded';
                }
            });
        } catch (err) {
            console.error(err);
            showToast(err.message || 'Ошибка при загрузке ресурсов.', 'error');
        }
    }

    function formatMoscowDate(raw) {
        if (!raw) return null;

        const dt = new Date(raw);
        if (Number.isNaN(dt.getTime())) {
            return null;
        }

        return dt.toLocaleString('ru-RU', {
            timeZone: 'Europe/Moscow',
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
        });
    }

    function createWatchPlaceholder(text, tone) {
        const card = document.createElement('div');
        card.className = 'admin-watch-card admin-watch-card--placeholder';
        if (tone === 'error') {
            card.classList.add('is-error');
        }
        card.textContent = text;
        return card;
    }

    function setupServerStatesSection() {
        const panel = document.querySelector('[data-role="server-states-panel"]');
        if (!panel) return;

        const grid = panel.querySelector('[data-role="server-states-grid"]');
        const updatedLabel = panel.querySelector('[data-role="server-states-updated"]');
        const endpoint = panel.dataset.endpoint;

        if (!grid || !endpoint) return;

        const renderError = (message) => {
            grid.innerHTML = '';
            grid.appendChild(createWatchPlaceholder(message || 'Не удалось загрузить данные.', 'error'));
            if (updatedLabel) {
                updatedLabel.textContent = '';
            }
        };

        const buildRow = (label, value) => {
            const row = document.createElement('div');
            row.className = 'admin-watch-card__row';

            const name = document.createElement('span');
            name.className = 'admin-watch-card__name';
            name.textContent = label;
            row.appendChild(name);

            const summary = document.createElement('span');
            summary.className = 'admin-watch-card__summary';
            summary.textContent = value;
            row.appendChild(summary);

            return row;
        };

        const buildTitle = (name, link) => {
            const tagName = link ? 'a' : 'div';
            const title = document.createElement(tagName);
            title.className = 'admin-watch-card__title';
            title.textContent = name || '—';

            if (link) {
                title.classList.add('admin-watch-card__title-link');
                title.href = link;
                title.target = '_blank';
                title.rel = 'noopener';
            }

            return title;
        };

        const renderServerStates = (items) => {
            grid.innerHTML = '';
            const states = Array.isArray(items) ? items : [];
            if (states.length === 0) {
                grid.appendChild(createWatchPlaceholder('Нет данных о серверах.'));
                return;
            }

            states.forEach((srv) => {
                const card = document.createElement('div');
                card.className = 'admin-watch-card';
                if (srv.error) {
                    card.classList.add('is-error');
                }

                card.appendChild(buildTitle(srv.name, srv.link));

                const meta = document.createElement('div');
                meta.className = 'admin-watch-card__meta';
                meta.textContent = srv.error
                    ? `Ошибка: ${srv.error}`
                    : `Обновлено ${srv.updated || '—'}`;
                card.appendChild(meta);

                const list = document.createElement('div');
                list.className = 'admin-watch-card__list';
                list.appendChild(buildRow('Круг', srv.cycle_avg || '—'));
                list.appendChild(buildRow('Ping', srv.ping ? 'OK' : '—'));
                list.appendChild(buildRow('GnBots', srv.gn ? 'OK' : '—'));
                const dnVal = srv.dn_count !== null && srv.dn_count !== undefined
                    ? srv.dn_count
                    : (srv.dn ? 1 : '—');
                list.appendChild(buildRow('LD', dnVal));

                card.appendChild(list);
                grid.appendChild(card);
            });
        };

        let serverStatesFirstLoad = true;

        const refreshServerStates = async () => {
            if (serverStatesFirstLoad) {
                grid.innerHTML = '';
                grid.appendChild(createWatchPlaceholder('Загружаем данные…'));
            }

            try {
                const resp = await fetch(endpoint, { headers: { Accept: 'application/json' } });
                if (!resp.ok) {
                    throw new Error(`HTTP ${resp.status}`);
                }

                const data = await resp.json();
                renderServerStates(data && data.items);

                serverStatesFirstLoad = false;

                const updatedAt = formatMoscowDate(data && data.generated_at);
                if (updatedLabel) {
                    updatedLabel.textContent = updatedAt ? `Обновлено ${updatedAt}` : '';
                }
            } catch (error) {
                console.error('[server-states] Не удалось обновить раздел:', error);
                renderError('Не удалось загрузить данные о серверах.');
            } finally {
                serverStatesFirstLoad = false;
            }
        };

        refreshServerStates();
        setInterval(refreshServerStates, 60_000);
    }

    function setupWatchCardsSection() {
        const panel = document.querySelector('[data-role="watch-cards-panel"]');
        if (!panel) return;

        const grid = panel.querySelector('[data-role="watch-cards-grid"]');
        const updatedLabel = panel.querySelector('[data-role="watch-cards-updated"]');
        const endpoint = panel.dataset.endpoint;

        if (!grid || !endpoint) return;

        const renderError = (message) => {
            grid.innerHTML = '';
            grid.appendChild(createWatchPlaceholder(message || 'Не удалось загрузить данные.', 'error'));
            if (updatedLabel) {
                updatedLabel.textContent = '';
            }
        };

        const renderWatchCards = (items) => {
            grid.innerHTML = '';
            const cards = Array.isArray(items) ? items : [];
            if (cards.length === 0) {
                grid.appendChild(createWatchPlaceholder('Проблем не обнаружено.'));
                return;
            }

            cards.forEach((cardData) => {
                const item = document.createElement('div');
                item.className = 'admin-watch-card';
                if (cardData.error) {
                    item.classList.add('is-error');
                }

                const title = document.createElement('div');
                title.className = 'admin-watch-card__title';
                title.textContent = cardData.server || '—';
                item.appendChild(title);

                const meta = document.createElement('div');
                meta.className = 'admin-watch-card__meta';
                const cardUpdated = formatMoscowDate(cardData.updated_raw || cardData.updated) || cardData.updated;
                if (cardData.error) {
                    meta.textContent = `Ошибка: ${cardData.error}`;
                } else if (cardUpdated) {
                    meta.textContent = `Обновлено ${cardUpdated}`;
                } else {
                    meta.textContent = 'Нет свежих данных';
                }
                item.appendChild(meta);

                const accounts = Array.isArray(cardData.accounts) ? cardData.accounts : [];
                if (accounts.length === 0) {
                    const empty = document.createElement('div');
                    empty.className = 'admin-watch-card__empty';
                    empty.textContent = 'Проблем не обнаружено';
                    item.appendChild(empty);
                } else {
                    const list = document.createElement('ul');
                    list.className = 'admin-watch-card__list';

                    accounts.forEach((acc) => {
                        const row = document.createElement('li');
                        row.className = 'admin-watch-card__row';

                        const name = document.createElement('span');
                        name.className = 'admin-watch-card__name';
                        name.textContent = acc.nickname || '—';
                        row.appendChild(name);

                        if (acc.summary) {
                            const summary = document.createElement('span');
                            summary.className = 'admin-watch-card__summary';
                            summary.textContent = acc.summary;
                            row.appendChild(summary);
                        }

                        list.appendChild(row);
                    });

                    item.appendChild(list);
                }

                grid.appendChild(item);
            });
        };

        let watchCardsFirstLoad = true;

        const refreshWatchCards = async () => {
            if (watchCardsFirstLoad) {
                grid.innerHTML = '';
                grid.appendChild(createWatchPlaceholder('Загружаем данные…'));
            }

            try {
                const resp = await fetch(endpoint, { headers: { Accept: 'application/json' } });
                if (!resp.ok) {
                    throw new Error(`HTTP ${resp.status}`);
                }

                const data = await resp.json();
                renderWatchCards(data && data.items);

                watchCardsFirstLoad = false;

                const updatedAt = formatMoscowDate(data && data.generated_at);
                if (updatedLabel) {
                    updatedLabel.textContent = updatedAt ? `Обновлено ${updatedAt}` : '';
                }
            } catch (error) {
                console.error('[watch-cards] Не удалось обновить раздел:', error);
                renderError('Не удалось загрузить сводку наблюдения.');
            } finally {
                watchCardsFirstLoad = false;
            }
        };

        refreshWatchCards();
        setInterval(refreshWatchCards, 60_000);
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

    const farmDataWatchedFields = new Set(["email", "password", "igg_id", "server", "telegram_tag"]);
    const farmDataAutoSaveDelay = 700;
    let farmDataSaveInProgress = false;
    let farmDataAutoSaveTimer = null;
    let farmDataAutoSaveQueued = false;
    let lastAutoSaveToastAt = 0;

    function clearFarmDataErrors(targetRow) {
        const table = document.querySelector('[data-role="farmdata-table"]');
        if (!table) return;

        const rows = targetRow ? [targetRow] : Array.from(table.querySelectorAll("tbody tr[data-account-id]"));
        rows.forEach((row) => {
            row.classList.remove("farmdata-row-error");
            row.querySelectorAll(".farmdata-input--error").forEach((input) => {
                input.classList.remove("farmdata-input--error");
            });
            row.querySelectorAll(".farmdata-error-message").forEach((message) => message.remove());
        });
    }

    function resolveFarmDataFieldName(message) {
        const text = String(message || "").toLowerCase();
        if (text.includes("igg")) return "igg_id";
        if (text.includes("email") || text.includes("e-mail") || text.includes("почт")) return "email";
        if (text.includes("парол")) return "password";
        if (text.includes("сервер")) return "server";
        if (text.includes("telegram")) return "telegram_tag";
        return null;
    }

    function renderFarmDataErrors(errors) {
        if (!Array.isArray(errors) || errors.length === 0) return;

        const table = document.querySelector('[data-role="farmdata-table"]');
        if (!table) return;

        errors.forEach((error) => {
            const accountId = error && error.account_id != null ? String(error.account_id) : "";
            if (!accountId) return;

            const row = table.querySelector(`tbody tr[data-account-id="${accountId}"]`);
            if (!row) return;

            row.classList.add("farmdata-row-error");
            const fieldName = resolveFarmDataFieldName(error.message);
            const input = fieldName ? row.querySelector(`input[name="${fieldName}"]`) : null;
            const messageEl = document.createElement("div");
            messageEl.className = "farmdata-error-message";
            messageEl.textContent = error.message || "Ошибка в строке.";

            if (input) {
                input.classList.add("farmdata-input--error");
                input.insertAdjacentElement("afterend", messageEl);
            } else {
                const cell = row.querySelector("td") || row;
                cell.appendChild(messageEl);
            }
        });
    }

    function setFarmDataSavingState(isSaving, isAuto) {
        const btn = document.querySelector('[data-action="farmdata-save"]');
        const textEl = btn ? findBtnTextEl(btn) : null;

        if (btn) {
            if (isSaving && textEl) {
                textEl.textContent = isAuto ? "Автосохранение..." : "Сохраняем...";
            }
            setButtonLoading(btn, isSaving);
            if (!isSaving && textEl) {
                textEl.textContent = "Сохранить изменения";
            }
        }
    }

    function collectClientFarmDataItems() {
        const table = document.querySelector('[data-role="farmdata-table"]');
        if (!table) {
            return { items: [], dirtyRows: [], totalRows: 0, missingTable: true };
        }

        const rows = Array.from(table.querySelectorAll("tbody tr[data-account-id]"));
        const dirtyRows = rows.filter((tr) => tr.dataset.dirty === "1");
        const items = [];

        dirtyRows.forEach(function (tr) {
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

        return { items, dirtyRows, totalRows: rows.length, missingTable: false };
    }

    async function handleFarmDataSave(btn, options = {}) {
        const isAuto = Boolean(options.isAuto);
        const {
            items,
            dirtyRows,
            totalRows,
            missingTable
        } = collectClientFarmDataItems();
        if (missingTable) {
            if (!isAuto) {
                showToast("Таблица с фермами не найдена.", "error");
            }
            return;
        }
        if (!totalRows) {
            if (!isAuto) {
                showToast("Нет ферм для сохранения.", "info");
            }
            return;
        }

        if (!items.length) {
            if (!isAuto) {
                showToast("Нет изменений для сохранения.", "info");
            }
            return;
        }

        if (isAuto) {
            if (farmDataSaveInProgress) {
                return;
            }
            farmDataSaveInProgress = true;
            setFarmDataSavingState(true, true);
        } else {
            setButtonLoading(btn, true);
            const textEl = btn ? findBtnTextEl(btn) : null;
            if (textEl) {
                textEl.textContent = "Сохраняем...";
            }
        }
        clearFarmDataErrors();
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
            const savedSet = new Set((data.saved || []).map((value) => String(value)));
            dirtyRows.forEach((row) => {
                if (!row) return;
                if (savedSet.has(String(row.dataset.accountId))) {
                    row.dataset.dirty = "0";
                }
            });
            renderFarmDataErrors(data.errors || []);
            const now = Date.now();
            if (!isAuto || now - lastAutoSaveToastAt > 8000) {
                const hasErrors = Array.isArray(data.errors) && data.errors.length > 0;
                if (hasErrors) {
                    showToast("Часть строк не сохранена. Проверьте подсветку.", "error");
                } else {
                    showToast(isAuto ? "Изменения сохранены автоматически." : "Данные ферм сохранены.", "success");
                }
                if (isAuto) {
                    lastAutoSaveToastAt = now;
                }
            }
        } catch (err) {
            console.error(err);
            showToast(err.message || "Ошибка при сохранении данных.", "error");
        } finally {
            if (isAuto) {
                farmDataSaveInProgress = false;
                setFarmDataSavingState(false);
                if (farmDataAutoSaveQueued) {
                    farmDataAutoSaveQueued = false;
                    scheduleFarmDataAutoSave();
                }
            } else {
                setButtonLoading(btn, false);
                const textEl = btn ? findBtnTextEl(btn) : null;
                if (textEl) {
                    textEl.textContent = "Сохранить изменения";
                }
            }
        }
    }

    function scheduleFarmDataAutoSave() {
        if (farmDataSaveInProgress) {
            farmDataAutoSaveQueued = true;
            return;
        }

        if (farmDataAutoSaveTimer) {
            clearTimeout(farmDataAutoSaveTimer);
        }

        farmDataAutoSaveTimer = setTimeout(function () {
            farmDataAutoSaveTimer = null;
            handleFarmDataSave(null, { isAuto: true });
        }, farmDataAutoSaveDelay);
    }

    function handleClientFarmDataChange(event) {
        const target = event.target;
        if (!target || target.tagName !== "INPUT") return;
        if (!farmDataWatchedFields.has(target.name)) return;

        const row = target.closest('[data-role="farmdata-table"] tbody tr[data-account-id]');
        if (!row) return;

        row.dataset.dirty = "1";
        clearFarmDataErrors(row);
        applyFarmDataStatusUI();
        scheduleFarmDataAutoSave();
    }

    document.addEventListener("input", handleClientFarmDataChange);
    document.addEventListener("change", handleClientFarmDataChange);

    document.addEventListener("DOMContentLoaded", function () {
        autoHideFlashMessages();
        setupNavToggle();
        setupAccountSearch();
        applyFarmDataStatusUI();
        loadAdminAccountResources();
        setupServerStatesSection();
        setupWatchCardsSection();
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
