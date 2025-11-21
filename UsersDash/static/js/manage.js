(function () {
    "use strict";

    const state = Object.assign({
        selectedAccountId: null,
        selectedAccountName: "",
        selectedServerName: "",
        steps: [],
        isLoading: false,
        detailsUrlTemplate: ""
    }, window.manageInitialState || {});

    const accountsRoot = document.querySelector('[data-role="manage-accounts"]');
    const stepsRoot = document.querySelector('[data-role="manage-steps"]');

    function replaceTemplate(str, accountId) {
        return (str || "").replace("__ACCOUNT__", accountId);
    }

    function showLoading() {
        if (!stepsRoot) return;
        stepsRoot.innerHTML = '<div class="manage-loading">Загружаем настройки...</div>';
    }

    function renderEmptyState(message) {
        if (!stepsRoot) return;
        stepsRoot.innerHTML = `<div class="manage-empty">${message || "Нет данных по шагам."}</div>`;
    }

    function renderSteps() {
        if (!stepsRoot) return;

        if (!state.selectedAccountId) {
            renderEmptyState("Выберите ферму слева, чтобы увидеть настройки.");
            return;
        }

        if (!state.steps || !state.steps.length) {
            renderEmptyState("Шаги для этой фермы не найдены.");
            return;
        }

        const html = state.steps
            .map(step => {
                const desc = step.description ? `<div class="step-desc">${step.description}</div>` : "";
                const schedule = step.schedule_summary
                    ? `<div class="step-schedule">⏱ ${step.schedule_summary}</div>`
                    : (step.schedule_rules_count ? `<div class="step-schedule muted">⏱ ${step.schedule_rules_count} правил расписания</div>` : "");

                const buttonText = step.is_active ? "Отключить" : "Включить";
                const statusBadge = step.is_active
                    ? '<span class="status-chip status-chip--ok">Активен</span>'
                    : '<span class="status-chip status-chip--warning">Выключен</span>';

                return `
                <div class="manage-step-card" data-step-idx="${step.index}">
                    <div class="manage-step-head">
                        <div>
                            <div class="step-title">${step.name || "Шаг"}</div>
                            ${desc}
                            ${schedule}
                        </div>
                        <div class="step-actions">
                            ${statusBadge}
                            <button class="btn btn-small btn-ghost"
                                    data-action="toggle-step"
                                    data-account-id="${state.selectedAccountId}"
                                    data-step-idx="${step.index}"
                                    data-current-active="${step.is_active ? "1" : "0"}">
                                <span class="btn-text">${buttonText}</span>
                                <span class="btn-spinner" hidden></span>
                            </button>
                        </div>
                    </div>
                </div>`;
            })
            .join("");

        stepsRoot.innerHTML = html;
    }

    function highlightAccount(accountId) {
        if (!accountsRoot) return;
        const items = accountsRoot.querySelectorAll('[data-account-id]');
        items.forEach((btn) => {
            const isActive = String(btn.dataset.accountId) === String(accountId);
            btn.classList.toggle("is-active", isActive);
        });
    }

    async function loadSteps(accountId) {
        if (!accountId || state.isLoading) return;
        state.isLoading = true;
        highlightAccount(accountId);
        showLoading();

        try {
            const url = replaceTemplate(state.detailsUrlTemplate, accountId);
            const resp = await fetch(url, { headers: { "x-skip-loader": "1" } });
            let data = {};
            try {
                data = await resp.json();
            } catch (_) {
                data = {};
            }

            if (!resp.ok || !data.ok) {
                throw new Error((data && data.error) || "Не удалось загрузить настройки.");
            }

            state.selectedAccountId = accountId;
            state.selectedAccountName = data.account && data.account.name;
            state.selectedServerName = data.account && data.account.server;
            state.steps = data.steps || [];
            renderSteps();
        } catch (err) {
            console.error(err);
            renderEmptyState(err.message);
        } finally {
            state.isLoading = false;
        }
    }

    function handleAccountClick(event) {
        const btn = event.target.closest('[data-account-id]');
        if (!btn) return;
        const accountId = btn.dataset.accountId;
        if (!accountId || String(accountId) === String(state.selectedAccountId)) return;
        loadSteps(accountId);
    }

    function init() {
        if (accountsRoot) {
            accountsRoot.addEventListener('click', handleAccountClick);
        }

        if (state.selectedAccountId) {
            highlightAccount(state.selectedAccountId);
            renderSteps();
        } else {
            renderEmptyState("Выберите ферму слева, чтобы увидеть настройки.");
        }
    }

    init();
})();
