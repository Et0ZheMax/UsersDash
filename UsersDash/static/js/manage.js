(function () {
    "use strict";

    const CONFIG_LABELS = {
        skip: "Пропуск",
        useResources: "Позволить боту использовать ресурсы из сумки",
        useSpeedUps: "Позволить боту использовать ускоры",
        marches: "Количество походов",
        ignoreSuicide: "Игнорировать предупреждение о тяжёлой битве",
        redMission: "Проходить красные миссии",
        highestMission: "Самая высокая миссия",
        lowestMission: "Самая низкая миссия",
        fastestMission: "Самая быстрая миссия",
        LevelStartAt: "Уровень начала",
        Monster: "Атаковать монстров",
        Niflung: "Нифлунги",
        Divine: "Божественные",
        Farm: "Еда",
        Sawmill: "Дерево",
        Quarry: "Камень",
        Gold: "Золото",
        RallyTime: "Время митинга",
        reduceLevel: "Снижать уровень",
        farmLowestResource: "Копать ресурс, которого меньше всего",
        quest: "Квесты",
        recruit: "Рекрутмент",
        vip: "VIP",
        worker: "Рабочий",
        gems: "Самоцветы",
        errands: "Поручения",
        specialFarmer: "Спец-фермер",
        skipVoyageLushLand: "Пропуск Lush Land",
        events: "События",
        collectCrystals: "Сбор кристаллов",
        allianceGift: "Подарки альянса",
        allianceDonation: "Пожертвования альянсу",
        Attack: "Атака",
        Defense: "Защита",
        Gather: "Сбор",
        Workers: "Рабочие",
        Deception: "Обман",
        Trade: "Торговля",
        Patrol: "Патруль",
        Infantry: "Пехота",
        Archer: "Лучники",
        Pikemen: "Копейщики",
        Porter: "Грузчики",
        Amount: "Количество тренируемых",
        UpgradeInfantry: "Апгрейд Пехоты",
        UpgradeArcher: "Апгрейд Лучников",
        UpgradePikemen: "Апгрейд Копейщиков",
        UpgradePorter: "Апгрейд Грузчиков",
        Upgrade: "Что улучшать",
        EagleNest: "Орлиное гнездо",
        Warehouse: "Склад",
        HallofValor: "Зал доблести",
        TribeHall: "Племенной зал",
        DivinationShack: "Гадальная хижина",
        Academy: "Академия",
        Watchtower: "Сторожевая башня",
        Infirmary: "Лазарет",
        SquadBase: "Штаб отрядов",
        VillageHall: "Ратуша поселения",
        Workshop: "Мастерская",
        Prison: "Тюрьма",
        DefenderCamp: "Лагерь защитников",
        SuppyHub: "Склад снабжения",
        Market: "Рынок",
        research: "Ветка исследований",
        upgrade: "Улучшаем исследования",
        SpeedUp: "Ускорения",
        Food: "Еда",
        Stones: "Камни",
        Gems: "Гемы",
        Lumber: "Дерево",
        ConstructionSpeed: "Скорость строительства",
        TrainExpansion: "Расширение обучения",
        ForgingSpeed: "Скорость ковки",
        ResearchSpeed: "Скорость исследований",
        TrainingSpeed: "Скорость тренировок",
        ForgingConsumption: "Расход ковки",
        HealingSpeed: "Скорость лечения",
        TrainingConsumption: "Расход тренировок",
        HealingConsumption: "Расход лечения",
    };

    const OPTION_LABELS = {
        worker: { Off: "Выкл", Common: "Обычный", Rare: "Редкий", Legend: "Легендарный", All: "Все" },
        Amount: { "100%": "100%", "75%": "75%", "50%": "50%", "25%": "25%" },
        RallyTime: { "5min": "5 минут", "10min": "10 минут", "30min": "30 минут", "8hours": "8 часов" },
        Upgrade: { MainHall: "Главное здание", Specfic: "Конкретное", Villages: "Посёлки" },
        research: { Economy: "Экономика", Military: "Армия" },
        allianceDonation: {
            Recommended: "Рекомендовано",
            Development: "Развитие",
            Territory: "Территория",
            War: "Война",
            Skills: "Навыки",
            Off: "Выкл",
        },
        "*": { Off: "Выкл", Any: "Любой", Auto: "Авто", on: "Вкл", off: "Выкл" },
    };

    const SCRIPT_LABELS = {
        "vikingbot.base.gathervip": "Сбор ресурсов",
        "vikingbot.base.dailies": "Ежедневные задания",
        "vikingbot.base.alliancedonation": "Техи и подарки племени",
        "vikingbot.base.mail": "Почта",
        "vikingbot.base.buffs": "Баффы",
        "vikingbot.base.recruitment": "Найм войск",
        "vikingbot.base.upgrade": "Стройка",
        "vikingbot.base.research": "Исследования",
        "vikingbot.base.divinationshack": "Хижина Гадалки",
        "vikingbot.base.exploration": "Экспедиции в поручениях (перья, яблоки)",
        "vikingbot.base.commission": "Выполнять поручения",
        "vikingbot.base.dragoncave": "Пещера дракона",
        "vikingbot.base.stagingpost": "Пост разгрузки",
        "vikingbot.base.build": "Строить новые здания (молоток)",
        "vikingbot.base.villages": "Сбор наград с орлов",
        "vikingbot.base.heal": "Лечение",
        "vikingbot.base.eaglenest": "Орлиное гнездо",
    };

    const ORDER_MAP = {
        "vikingbot.base.gathervip": [
            "Farm", "Quarry", "Sawmill", "Gold", "reduceLevel", "farmLowestResource",
            "LevelStartAt", "RallyTime", "Monster", "Niflung", "marches", "Divine",
        ],
        "vikingbot.base.dailies": [
            "quest", "recruit", "vip", "gems", "errands", "worker", "specialFarmer", "skipVoyageLushLand", "events", "collectCrystals",
        ],
        "vikingbot.base.alliancedonation": ["allianceGift", "allianceDonation"],
        "vikingbot.base.buffs": ["Gather", "Workers", "Attack", "Defense", "Patrol", "Trade", "Deception", "useGems"],
        "vikingbot.base.recruitment": [
            "Infantry", "Archer", "Pikemen", "Porter", "Amount",
            "UpgradeInfantry", "UpgradeArcher", "UpgradePikemen", "UpgradePorter",
            "useResources", "useSpeedUps",
        ],
        "vikingbot.base.upgrade": [
            "Upgrade", "useResources", "useSpeedUps", "TribeHall", "VillageHall", "MainHall",
            "EagleNest", "Warehouse", "HallofValor", "DivinationShack", "Academy", "Watchtower",
            "Infirmary", "SquadBase", "Workshop", "Prison", "DefenderCamp", "SuppyHub", "Market",
        ],
        "vikingbot.base.research": ["research", "upgrade", "useResources", "useSpeedUps"],
        "vikingbot.base.divinationshack": [
            "Gold", "Gems", "Food", "Lumber", "Stones", "SpeedUp", "ConstructionSpeed", "ResearchSpeed",
            "TrainingSpeed", "HealingSpeed", "ForgingSpeed", "TrainExpansion", "ForgingConsumption",
            "TrainingConsumption", "HealingConsumption",
        ],
    };

    const state = Object.assign({
        selectedAccountId: null,
        selectedAccountName: "",
        selectedServerName: "",
        steps: [],
        rawSteps: [],
        debugInfo: null,
        menu: {},
        isLoading: false,
        detailsUrlTemplate: "",
        toggleUrlTemplate: "",
        updateUrlTemplate: "",
        selectedStepIndex: null,
    }, window.manageInitialState || {});

    state.detailsUrlTemplate = state.detailsUrlTemplate || "/manage/account/__ACCOUNT__/details";
    state.toggleUrlTemplate = state.toggleUrlTemplate || "/account/__ACCOUNT__/settings/step/__STEP__/toggle";
    state.updateUrlTemplate = state.updateUrlTemplate || "/manage/account/__ACCOUNT__/settings/__STEP__";

    const accountsRoot = document.querySelector('[data-role="manage-accounts"]');
    const stepsRoot = document.querySelector('[data-role="manage-steps"]');
    const configRoot = document.querySelector('[data-role="manage-config"]');
    const stepsTitleEl = document.querySelector('[data-role="steps-title"]');
    const stepsSubtitleEl = document.querySelector('[data-role="steps-subtitle"]');
    const configTitleEl = document.querySelector('[data-role="config-title"]');
    const configSubtitleEl = document.querySelector('[data-role="config-subtitle"]');
    const mobileNavTitle = document.querySelector('[data-role="mobile-title"]');
    const mobileNavSubtitle = document.querySelector('[data-role="mobile-subtitle"]');
    const mobileBackBtn = document.querySelector('[data-role="mobile-back"]');
    const layoutRoot = document.querySelector('[data-role="manage-layout"]');
    const manageRoot = document.querySelector('.manage-modern');

    function escapeHtml(str) {
        return (str || "").replace(/[&<>"]+/g, (ch) => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
        }[ch] || ch));
    }

    function replaceTemplate(str, accountId) {
        return (str || "").replace("__ACCOUNT__", accountId);
    }

    function replaceStepTemplate(str, accountId, stepIdx) {
        let url = replaceTemplate(str, accountId);
        if (stepIdx !== undefined && stepIdx !== null) {
            url = url.replace("__STEP__", stepIdx);
        }
        return url;
    }

    function getScriptTitle(step) {
        const cfg = (step && step.Config) || {};
        const scriptId = step && step.ScriptId;
        return cfg.Name || cfg.name || SCRIPT_LABELS[scriptId] || scriptId || "Шаг";
    }

    function formatScheduleRule(rule) {
        if (!rule || typeof rule !== "object") return null;
        const start = rule.StartAt || rule.Start || rule.From || rule.TimeFrom;
        const end = rule.EndAt || rule.End || rule.To || rule.TimeTo;
        const every = rule.Every || rule.Interval || rule.EveryMinutes;
        const days = rule.Days || rule.WeekDays || rule.Weekdays;
        const label = rule.Label || rule.Name;
        const parts = [];
        if (days) {
            parts.push(Array.isArray(days) ? `Дни: ${days.join(", ")}` : `Дни: ${days}`);
        }
        if (start || end) {
            parts.push(`${start || "00:00"} — ${end || "24:00"}`);
        }
        if (every) {
            parts.push(`каждые ${every}`);
        }
        if (label) {
            parts.push(label);
        }
        return parts.join("; ") || null;
    }

    function scheduleSummary(stepView, rawStep) {
        if (stepView && stepView.schedule_summary) return stepView.schedule_summary;
        const rules = (rawStep && rawStep.ScheduleRules) || [];
        const summaries = rules.map(formatScheduleRule).filter(Boolean);
        if (summaries.length) return summaries.join("; ");
        if (rules.length) return `${rules.length} правил расписания`;
        return "";
    }

    function buildViewStepsFromRaw(rawSteps) {
        return (rawSteps || []).map((step, idx) => {
            const cfg = (step && step.Config) || {};
            const name = getScriptTitle(step) || `Шаг ${idx + 1}`;
            const description = cfg.Description || cfg.description || "";
            const schedule_rules = (step && Array.isArray(step.ScheduleRules)) ? step.ScheduleRules : [];
            return {
                index: idx,
                name,
                script_id: step && step.ScriptId,
                config: cfg,
                description,
                is_active: step && typeof step.IsActive === "boolean" ? step.IsActive : true,
                schedule_summary: scheduleSummary(null, step) || undefined,
                schedule_rules_count: schedule_rules.length,
            };
        });
    }

    function renderEmptyState(message, debug) {
        let extra = "";
        if (debug) {
            const asJson = escapeHtml(JSON.stringify(debug, null, 2));
            extra = `<details class="manage-debug"><summary>Диагностика загрузки</summary><pre>${asJson}</pre></details>`;
        }
        if (stepsRoot) stepsRoot.innerHTML = `<div class="manage-empty">${message || "Нет данных по шагам."}${extra}</div>`;
        if (configRoot) configRoot.innerHTML = "";
        updateHeaderText();
    }

    function updateHeaderText() {
        if (stepsTitleEl) {
            stepsTitleEl.textContent = state.selectedAccountName || "Выберите ферму";
        }
        if (stepsSubtitleEl) {
            stepsSubtitleEl.textContent = state.selectedAccountId
                ? (state.selectedServerName || "Сервер не указан")
                : "Выберите ферму, чтобы увидеть шаги";
        }

        const selectedStep = (state.selectedStepIndex !== null && state.rawSteps[state.selectedStepIndex])
            ? state.rawSteps[state.selectedStepIndex]
            : null;

        if (configTitleEl) {
            configTitleEl.textContent = selectedStep ? getScriptTitle(selectedStep) : "Выберите шаг";
        }
        if (configSubtitleEl) {
            configSubtitleEl.textContent = selectedStep
                ? (state.selectedAccountName || "")
                : "Параметры появятся после выбора шага";
        }
    }

    function renderSteps() {
        if (!stepsRoot) return;

        if (!state.selectedAccountId) {
            renderEmptyState("Выберите ферму слева, чтобы увидеть настройки.");
            return;
        }

        if (!state.rawSteps || !state.rawSteps.length) {
            renderEmptyState("Шаги для этой фермы не найдены.", state.debugInfo);
            return;
        }

        const html = state.rawSteps.map((rawStep, idx) => {
            const viewStep = state.steps[idx] || {};
            const desc = viewStep.description ? `<div class=\"step-desc\">${viewStep.description}</div>` : "";
            const schedule = scheduleSummary(viewStep, rawStep);
            const scheduleHtml = schedule ? `<div class=\"step-schedule\">⏱ ${schedule}</div>` : "";
            const switchId = `step-toggle-${state.selectedAccountId || "acc"}-${idx}`;
            const isSelected = state.selectedStepIndex === idx;
            const name = getScriptTitle(rawStep) || viewStep.name || `Шаг ${idx + 1}`;

            return `
                <div class="manage-step-card ${isSelected ? "is-selected" : ""}" data-step-idx="${idx}">
                    <div class="manage-step-head">
                        <div>
                            <div class="step-title">${name}</div>
                            ${desc}
                            ${scheduleHtml}
                        </div>
                        <div class="step-actions">
                            <label class="ios-switch" for="${switchId}">
                                <input type="checkbox"
                                       id="${switchId}"
                                       class="ios-switch__input"
                                       data-role="step-toggle"
                                        data-account-id="${state.selectedAccountId}"
                                       data-step-idx="${idx}"
                                       ${viewStep.is_active ? "checked" : ""}>
                                <span class="ios-switch__slider" aria-hidden="true"></span>
                            </label>
                        </div>
                    </div>
                </div>`;
        }).join("");

        stepsRoot.innerHTML = html;
        updateHeaderText();
    }

    let currentConfigForm = null;
    let configAutoSaveTimer = null;
    let configSaveInProgress = false;
    let configAutoSaveQueued = false;
    let lastConfigToastAt = 0;

    function renderConfig() {
        if (!configRoot) return;
        configRoot.innerHTML = "";

        if (configAutoSaveTimer) {
            clearTimeout(configAutoSaveTimer);
            configAutoSaveTimer = null;
        }

        if (state.selectedStepIndex === null || !state.rawSteps[state.selectedStepIndex]) {
            configRoot.innerHTML = '<div class="config-empty">Выберите шаг, чтобы редактировать настройки.</div>';
            updateHeaderText();
            return;
        }

        const step = state.rawSteps[state.selectedStepIndex];
        const cfg = step.Config || {};
        const title = getScriptTitle(step);
        const subtitle = state.selectedAccountName || "";

        const header = document.createElement("div");
        header.className = "config-header";
        header.innerHTML = `
            <div>
                <div class="config-title">${title}</div>
                <div class="config-subtitle">${subtitle}</div>
            </div>
            <span class="status-chip ${step.IsActive ? "status-chip--ok" : "status-chip--warning"}">
                ${step.IsActive ? "Активен" : "Выключен"}
            </span>
        `;
        configRoot.appendChild(header);

        const form = document.createElement("div");
        form.className = "config-form";

        const orderedKeys = (() => {
            const keys = Object.keys(cfg || {});
            const preferred = ORDER_MAP[step.ScriptId] || [];
            const seen = new Set(preferred);
            const rest = keys.filter((k) => !seen.has(k)).sort((a, b) => a.localeCompare(b, "ru"));
            return [...preferred.filter((k) => keys.includes(k)), ...rest];
        })();

        if (!orderedKeys.length) {
            form.innerHTML = '<div class="config-empty">Нет параметров для этого шага.</div>';
        }

        orderedKeys.forEach((key) => {
            const conf = cfg[key];
            const field = document.createElement("div");
            field.className = "config-field";
            const label = CONFIG_LABELS[key] || key;

            if (typeof conf === "boolean") {
                field.classList.add("config-field--boolean");
                field.innerHTML = `
                    <label class="config-checkbox" for="cfg_${key}">
                        <input type="checkbox" class="config-checkbox__input" id="cfg_${key}" name="${key}" ${conf ? "checked" : ""}>
                        <span class="config-checkbox__box" aria-hidden="true"></span>
                        <span class="config-checkbox__label">${label}</span>
                    </label>`;
            } else if (conf && typeof conf === "object" && Array.isArray(conf.options)) {
                const options = conf.options
                    .map((opt) => {
                        const text = (OPTION_LABELS[key] && OPTION_LABELS[key][opt])
                            || (OPTION_LABELS["*"] && OPTION_LABELS["*"][opt])
                            || opt;
                        const selected = opt === conf.value ? "selected" : "";
                        return `<option value="${opt}" ${selected}>${text}</option>`;
                    }).join("");
                field.innerHTML = `
                    <label for="cfg_${key}">${label}</label>
                    <select id="cfg_${key}" name="${key}">${options}</select>`;
            } else {
                const value = conf ?? "";
                const isNumber = typeof conf === "number";
                field.innerHTML = `
                    <label for="cfg_${key}">${label}</label>
                    <input type="${isNumber ? "number" : "text"}" id="cfg_${key}" name="${key}" value="${value}">`;
            }

            form.appendChild(field);
        });

        const footer = document.createElement("div");
        footer.className = "config-footer";
        const saveBtn = document.createElement("button");
        saveBtn.className = "btn btn-primary btn-small";
        saveBtn.textContent = "Сохранить сейчас";
        saveBtn.addEventListener("click", () => saveConfig(state.selectedStepIndex, form, cfg));
        footer.appendChild(saveBtn);
        form.appendChild(footer);

        form.dataset.stepIdx = String(state.selectedStepIndex);
        form.addEventListener("input", () => scheduleConfigAutoSave(state.selectedStepIndex, form, cfg));
        form.addEventListener("change", () => scheduleConfigAutoSave(state.selectedStepIndex, form, cfg));

        currentConfigForm = form;

        configRoot.appendChild(form);
        updateHeaderText();
    }

    function highlightAccount(accountId) {
        if (!accountsRoot) return;
        const items = accountsRoot.querySelectorAll('[data-account-id]');
        items.forEach((btn) => {
            const isActive = String(btn.dataset.accountId) === String(accountId);
            btn.classList.toggle("is-active", isActive);
        });
    }

    async function toggleStep(accountId, stepIdx, nextActive, control) {
        if (!accountId) return;
        const desiredState = Boolean(nextActive);
        if (control) control.disabled = true;
        try {
            const url = replaceStepTemplate(state.toggleUrlTemplate, accountId, stepIdx);
            const resp = await fetch(url, {
                method: "POST",
                headers: { "Content-Type": "application/json", "x-skip-loader": "1" },
                body: JSON.stringify({ is_active: desiredState }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) throw new Error(data.error || "Ошибка сохранения");
            if (state.steps[stepIdx]) state.steps[stepIdx].is_active = desiredState;
            if (state.rawSteps[stepIdx]) state.rawSteps[stepIdx].IsActive = desiredState;
            renderSteps();
            renderConfig();
        } catch (err) {
            console.error(err);
            alert(err.message || "Не удалось обновить шаг");
        } finally {
            if (control) control.disabled = false;
        }
    }

    function collectConfig(formEl, cfg) {
        const result = {};
        const inputs = formEl.querySelectorAll("input, select");
        inputs.forEach((input) => {
            const key = input.name;
            if (!key) return;
            const original = cfg[key];
            if (input.type === "checkbox") {
                result[key] = input.checked;
            } else if (input.tagName === "SELECT") {
                result[key] = input.value;
            } else if (typeof original === "number") {
                const n = Number(input.value);
                result[key] = Number.isFinite(n) ? n : original;
            } else {
                result[key] = input.value;
            }
        });
        return result;
    }

    function showMiniToast(message, type = "info") {
        let container = document.querySelector(".mini-toast-container");
        if (!container) {
            container = document.createElement("div");
            container.className = "mini-toast-container";
            document.body.appendChild(container);
        }

        const toast = document.createElement("div");
        toast.className = `mini-toast mini-toast--${type}`;
        toast.textContent = message;
        container.innerHTML = "";
        container.appendChild(toast);

        requestAnimationFrame(() => toast.classList.add("is-visible"));
        setTimeout(() => toast.classList.remove("is-visible"), 1600);
        setTimeout(() => toast.remove(), 2000);
    }

    function scheduleConfigAutoSave(stepIdx, formEl, cfg) {
        if (!formEl || !cfg || formEl !== currentConfigForm) return;
        if (Number(formEl.dataset.stepIdx) !== stepIdx) return;

        if (configAutoSaveTimer) {
            clearTimeout(configAutoSaveTimer);
        }

        configAutoSaveTimer = setTimeout(() => {
            configAutoSaveTimer = null;
            saveConfig(stepIdx, formEl, cfg, { isAuto: true });
        }, 650);
    }

    async function saveConfig(stepIdx, formEl, cfg, options = {}) {
        const isAuto = Boolean(options.isAuto);
        if (!state.selectedAccountId) return;
        if (isAuto && configSaveInProgress) {
            configAutoSaveQueued = true;
            return;
        }
        if (isAuto) {
            configSaveInProgress = true;
        }
        if (configAutoSaveTimer) {
            clearTimeout(configAutoSaveTimer);
            configAutoSaveTimer = null;
        }
        const payload = collectConfig(formEl, cfg || {});
        try {
            const url = replaceStepTemplate(state.updateUrlTemplate, state.selectedAccountId, stepIdx);
            const resp = await fetch(url, {
                method: "PUT",
                headers: { "Content-Type": "application/json", "x-skip-loader": "1" },
                body: JSON.stringify({ Config: payload }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) throw new Error(data.error || "Ошибка сохранения");
            if (state.rawSteps[stepIdx]) {
                state.rawSteps[stepIdx].Config = Object.assign({}, state.rawSteps[stepIdx].Config, payload);
            }
            const now = Date.now();
            if (!isAuto || now - lastConfigToastAt > 4000) {
                showMiniToast("Сохранено", "success");
                lastConfigToastAt = now;
            }
            renderConfig();
        } catch (err) {
            console.error(err);
            alert(err.message || "Не удалось сохранить настройки");
        } finally {
            if (isAuto) {
                configSaveInProgress = false;
                if (configAutoSaveQueued) {
                    configAutoSaveQueued = false;
                    scheduleConfigAutoSave(stepIdx, formEl, cfg);
                }
            }
        }
    }

    function extractStepsAndMenu(payload) {
        if (!payload) return { steps: [], menu: {} };

        const asListFromMapping = (obj) => {
            const keys = Object.keys(obj || {});
            if (keys.length && keys.every((k) => /^\d+$/.test(k))) {
                return keys.sort((a, b) => Number(a) - Number(b)).map((k) => obj[k]);
            }
            return null;
        };

        const safeMenu = (obj, fallback = {}) => {
            if (!obj || typeof obj !== "object") return fallback || {};
            const menu = obj.MenuData || obj.menu || obj.menu_data || fallback || {};
            return (menu && typeof menu === "object") ? menu : {};
        };

        const safeSteps = (val) => {
            if (Array.isArray(val)) return val;
            if (val && typeof val === "object") {
                const nested = val.Data || val.data || val.steps || val.Steps;
                if (Array.isArray(nested)) return nested;
                if (nested && typeof nested === "object") {
                    const mapped = asListFromMapping(nested);
                    if (mapped) return mapped;
                }

                // одиночный шаг без обёртки
                if ("Config" in val || "config" in val || "ScriptId" in val || "script_id" in val) {
                    return [val];
                }

                const mappedSelf = asListFromMapping(val);
                if (mappedSelf) return mappedSelf;
            }
            return [];
        };

        if (Array.isArray(payload)) {
            return { steps: payload, menu: {} };
        }

        if (payload && typeof payload === "object") {
            const primary = payload.Data || payload.data || payload;
            let steps = safeSteps(primary);
            let menu = safeMenu(primary, safeMenu(payload));
            if (!steps.length && primary && typeof primary === "object") {
                const nested = primary.Data || primary.data;
                steps = safeSteps(nested);
                if (!Object.keys(menu || {}).length) {
                    menu = safeMenu(nested, menu);
                }
            }
            return { steps, menu };
        }

        return { steps: [], menu: {} };
    }

    async function loadSteps(accountId, meta = {}) {
        if (!accountId || state.isLoading) return;
        state.isLoading = true;
        if (meta.name) state.selectedAccountName = meta.name;
        if (meta.server) state.selectedServerName = meta.server;
        highlightAccount(accountId);
        renderEmptyState("Загружаем настройки...");

        try {
            const url = replaceTemplate(state.detailsUrlTemplate, accountId);
            const resp = await fetch(url, { headers: { "x-skip-loader": "1" } });
            let data = {};
            try { data = await resp.json(); } catch (_) { data = {}; }

            const isOk = resp.ok && (data.ok !== false);
            if (!isOk) {
                throw new Error((data && data.error) || "Не удалось загрузить настройки.");
            }

            const normalized = extractStepsAndMenu(data.raw_steps || data.rawSteps || data.Data || data.data || data);
            const rawSteps = normalized.steps;
            const viewSteps = data.steps || data.view_steps || buildViewStepsFromRaw(rawSteps);
            const menu = data.menu || data.MenuData || data.menu_data || normalized.menu;
            const account = data.account || {};
            const debug = data.debug || null;

            console.debug("Manage: ответ settings", {
                accountId,
                status: resp.status,
                payloadKeys: data ? Object.keys(data) : [],
                debug,
                normalizedSteps: Array.isArray(rawSteps) ? rawSteps.length : 0,
            });

            state.selectedAccountId = accountId;
            state.selectedAccountName = account.name || state.selectedAccountName || meta.name || "";
            state.selectedServerName = account.server || state.selectedServerName || meta.server || "";
            state.steps = viewSteps || [];
            state.rawSteps = rawSteps || [];
            state.menu = menu;
            state.debugInfo = debug || {
                http_status: resp.status,
                payload_keys: data ? Object.keys(data) : [],
                normalized_steps: Array.isArray(rawSteps) ? rawSteps.length : 0,
            };

            const startOnMobile = isMobile();
            state.selectedStepIndex = (state.rawSteps.length && !startOnMobile) ? 0 : null;
            if (!state.rawSteps.length) {
                renderEmptyState("Настройки не найдены в ответе сервера.", state.debugInfo);
            } else {
                renderSteps();
                renderConfig();
                updateHeaderText();
                if (startOnMobile) {
                    setMobileView('steps');
                }
            }
        } catch (err) {
            console.error(err);
            state.debugInfo = state.debugInfo || { error: err && err.message };
            renderEmptyState(err.message, state.debugInfo);
        } finally {
            state.isLoading = false;
        }
    }

    function handleAccountClick(event) {
        const btn = event.target.closest('[data-account-id]');
        if (!btn) return;
        if (event.preventDefault) {
            event.preventDefault();
        }
        const accountId = btn.dataset.accountId;
        if (!accountId) return;
        loadSteps(accountId, {
            name: btn.dataset.accountName,
            server: btn.dataset.serverName,
        });
        if (isMobile()) {
            setMobileView('steps');
        }
    }

    function handleStepToggleChange(event) {
        const toggleInput = event.target.closest('input[data-role="step-toggle"]');
        if (!toggleInput) return;
        event.stopPropagation();
        const stepIdx = Number(toggleInput.dataset.stepIdx);
        if (Number.isNaN(stepIdx)) return;
        toggleStep(
            toggleInput.dataset.accountId,
            stepIdx,
            toggleInput.checked,
            toggleInput
        );
    }

    function handleStepsClick(event) {
        if (event.target.closest('.ios-switch')) {
            event.stopPropagation();
            return;
        }

        const card = event.target.closest('[data-step-idx]');
        if (!card) return;
        const idx = Number(card.dataset.stepIdx);
        if (Number.isNaN(idx)) return;
        state.selectedStepIndex = idx;
        renderSteps();
        renderConfig();
        if (isMobile()) {
            setMobileView('config');
        }
    }

    function isMobile() {
        return window.matchMedia('(max-width: 960px)').matches;
    }

    function setMobileView(nextView) {
        if (!manageRoot || !nextView) return;
        state.mobileView = nextView;
        manageRoot.setAttribute('data-mobile-view', nextView);
        if (mobileNavTitle) {
            if (nextView === 'accounts') {
                mobileNavTitle.textContent = 'Фермы';
            } else if (nextView === 'steps') {
                mobileNavTitle.textContent = state.selectedAccountName || 'Шаги';
            } else {
                mobileNavTitle.textContent = 'Конфигурация';
            }
        }
        if (mobileNavSubtitle) {
            if (nextView === 'steps') {
                mobileNavSubtitle.textContent = state.selectedServerName || '';
            } else if (nextView === 'config') {
                const step = state.rawSteps && state.rawSteps[state.selectedStepIndex];
                mobileNavSubtitle.textContent = getScriptTitle(step) || '';
            } else {
                mobileNavSubtitle.textContent = '';
            }
        }
        if (mobileBackBtn) {
            mobileBackBtn.style.visibility = nextView === 'accounts' ? 'hidden' : 'visible';
        }
    }

    function mobileBack() {
        if (!state.mobileView) return;
        if (state.mobileView === 'config') {
            setMobileView('steps');
        } else if (state.mobileView === 'steps') {
            setMobileView('accounts');
        }
    }

    function bindSwipeNavigation() {
        if (!layoutRoot) return;
        let startX = null;
        let startY = null;

        layoutRoot.addEventListener('touchstart', (event) => {
            const touch = event.touches && event.touches[0];
            if (!touch) return;
            startX = touch.clientX;
            startY = touch.clientY;
        });

        layoutRoot.addEventListener('touchend', (event) => {
            if (startX === null || startY === null) return;
            const touch = event.changedTouches && event.changedTouches[0];
            if (!touch) return;
            const deltaX = touch.clientX - startX;
            const deltaY = Math.abs(touch.clientY - startY);
            startX = null;
            startY = null;
            if (Math.abs(deltaX) < 60 || deltaY > 50) return;
            if (deltaX > 0 && isMobile()) {
                mobileBack();
            }
        });
    }

    function handleResize() {
        if (!isMobile()) {
            if (manageRoot) manageRoot.removeAttribute('data-mobile-view');
            state.mobileView = null;
            return;
        }
        setMobileView(state.mobileView || 'accounts');
    }

    function init() {
        if (accountsRoot) {
            accountsRoot.addEventListener('click', handleAccountClick);
        }
        if (stepsRoot) {
            stepsRoot.addEventListener('click', handleStepsClick);
            stepsRoot.addEventListener('change', handleStepToggleChange);
        }
        if (mobileBackBtn) {
            mobileBackBtn.addEventListener('click', mobileBack);
        }

        bindSwipeNavigation();
        window.addEventListener('resize', handleResize);

        if (state.rawSteps === undefined && state.raw_steps) {
            state.rawSteps = state.raw_steps;
        }
        if (!state.rawSteps && state.steps && state.steps.length && Array.isArray(state.raw_steps)) {
            state.rawSteps = state.raw_steps;
        }
        if (!state.steps && state.view_steps) {
            state.steps = state.view_steps;
        }

        // Если ничего не подгружено сервером (или пришёл пустой массив),
        // подгружаем настройки через AJAX, чтобы правая часть не оставалась пустой.
        if (state.selectedAccountId && (!state.rawSteps || !state.rawSteps.length)) {
            loadSteps(state.selectedAccountId);
            return;
        }

        if (state.selectedAccountId) {
            highlightAccount(state.selectedAccountId);
            if (state.rawSteps && state.rawSteps.length && state.selectedStepIndex === null && !isMobile()) {
                state.selectedStepIndex = 0;
            }
            renderSteps();
            renderConfig();
        } else {
            renderEmptyState("Выберите ферму слева, чтобы увидеть настройки.");
        }

        const initialMobileView = isMobile()
            ? 'accounts'
            : (state.selectedAccountId
                ? (state.selectedStepIndex !== null ? 'config' : 'steps')
                : 'accounts');

        if (isMobile()) {
            setMobileView(initialMobileView);
        } else if (manageRoot) {
            manageRoot.removeAttribute('data-mobile-view');
            state.mobileView = null;
        }
    }

    init();
})();
