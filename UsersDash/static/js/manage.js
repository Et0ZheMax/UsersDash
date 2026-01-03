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

    const SCRIPT_LABELS_DEFAULT = {
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

    let SCRIPT_LABELS = Object.assign({}, SCRIPT_LABELS_DEFAULT);

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

    const STEP_HIDDEN_KEY = "__step__";

    const state = Object.assign({
        selectedAccountId: null,
        selectedAccountName: "",
        selectedServerName: "",
        selectedAccountTariffPrice: null,
        selectedAccountTariffName: "",
        selectedAccountHasDefaults: false,
        steps: [],
        rawSteps: [],
        visibilityMap: {},
        scriptLabels: {},
        debugInfo: null,
        menu: {},
        isLoading: false,
        detailsUrlTemplate: "",
        toggleUrlTemplate: "",
        updateUrlTemplate: "",
        accountToggleUrlTemplate: "",
        applyDefaultsUrlTemplate: "",
        copySettingsUrlTemplate: "",
        selectedStepIndex: null,
        scheduleDrafts: {},
        templatesCache: {},
        templatesLoading: false,
    }, window.manageInitialState || {});

    function applyScriptLabels(newLabels) {
        const safeLabels = (newLabels && typeof newLabels === "object") ? newLabels : {};
        state.scriptLabels = Object.assign({}, state.scriptLabels || {}, safeLabels);
        SCRIPT_LABELS = Object.assign({}, SCRIPT_LABELS_DEFAULT, state.scriptLabels || {});
    }

    applyScriptLabels(state.scriptLabels);

    state.detailsUrlTemplate = state.detailsUrlTemplate || "/manage/account/__ACCOUNT__/details";
    state.toggleUrlTemplate = state.toggleUrlTemplate || "/account/__ACCOUNT__/settings/step/__STEP__/toggle";
    state.updateUrlTemplate = state.updateUrlTemplate || "/manage/account/__ACCOUNT__/settings/__STEP__";
    state.accountToggleUrlTemplate = state.accountToggleUrlTemplate || "/manage/account/__ACCOUNT__/toggle-active";
    state.applyDefaultsUrlTemplate = state.applyDefaultsUrlTemplate || "/manage/account/__ACCOUNT__/apply-defaults";
    state.copySettingsUrlTemplate = state.copySettingsUrlTemplate || "/manage/account/__ACCOUNT__/copy-settings";

    const accountsRoot = document.querySelector('[data-role="manage-accounts"]');
    const stepsRoot = document.querySelector('[data-role="manage-steps"]');
    const configRoot = document.querySelector('[data-role="manage-config"]');
    const stepsTitleEl = document.querySelector('[data-role="steps-title"]');
    const stepsSubtitleEl = document.querySelector('[data-role="steps-subtitle"]');
    const configTitleEl = document.querySelector('[data-role="config-title"]');
    const configSubtitleEl = document.querySelector('[data-role="config-subtitle"]');
    const accountSearchInput = document.querySelector('[data-role="account-search"]');
    const accountsSearchEmpty = document.querySelector('[data-role="accounts-search-empty"]');
    const defaultsToolbarBtn = document.querySelector('[data-role="apply-defaults"]');
    const defaultsModal = document.querySelector('[data-role="defaults-modal"]');
    const defaultsConfirmBtn = document.querySelector('[data-role="defaults-confirm"]');
    const defaultsModalClosers = Array.from(document.querySelectorAll('[data-role="defaults-modal-close"]'));
    const templateControls = document.querySelector('[data-role="template-controls"]');
    const templateSelect = document.querySelector('[data-role="template-select"]');
    const templateApplyBtn = document.querySelector('[data-role="template-apply"]');
    const copyOpenBtn = document.querySelector('[data-role="copy-settings-open"]');
    const copyModal = document.querySelector('[data-role="copy-settings-modal"]');
    const copyCloseBtns = Array.from(document.querySelectorAll('[data-role="copy-settings-close"]'));
    const copyTabs = Array.from(document.querySelectorAll('[data-role="copy-settings-tab"]'));
    const copySubtitle = document.querySelector('[data-role="copy-settings-subtitle"]');
    const copyNoteSameServer = document.querySelector('[data-role="copy-note-same-server"]');
    const copyNoteCrossServer = document.querySelector('[data-role="copy-note-cross-server"]');
    const copySourceSelect = document.querySelector('[data-role="copy-source-select"]');
    const copyTargetsWrap = document.querySelector('[data-role="copy-targets"]');
    const copyTargetsAll = document.querySelector('[data-role="copy-targets-all"]');
    const copyTargetsHint = document.querySelector('[data-role="copy-targets-hint"]');
    const copySourceMeta = document.querySelector('[data-role="copy-source-meta"]');
    const copyConfirmInput = document.querySelector('[data-role="copy-confirm"]');
    const copyStatus = document.querySelector('[data-role="copy-status"]');
    const copyConfirmBtn = document.querySelector('[data-role="copy-settings-confirm"]');
    const adminAccounts = Array.isArray(window.manageAdminAccounts) ? window.manageAdminAccounts : [];
    const mobileNavTitle = document.querySelector('[data-role="mobile-title"]');
    const mobileNavSubtitle = document.querySelector('[data-role="mobile-subtitle"]');
    const mobileBackBtn = document.querySelector('[data-role="mobile-back"]');
    const layoutRoot = document.querySelector('[data-role="manage-layout"]');
    const manageRoot = document.querySelector('.manage-modern');
    const explicitAdminFlag = (typeof window.manageIsAdmin !== "undefined") ? window.manageIsAdmin : false;
    const isAdminManage = Boolean(
        explicitAdminFlag
        || (window.manageInitialState && window.manageInitialState.menu && window.manageInitialState.menu.is_admin_manage)
        || (window.manageInitialState && window.manageInitialState.is_admin_manage)
        || ((window.location && window.location.pathname) ? window.location.pathname.includes('/admin/') : false)
    );
    const paymentWarning = document.querySelector('[data-role="payment-warning"]');

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

    function unwrapValue(raw) {
        let current = raw;
        const visited = new Set();
        while (
            current
            && typeof current === "object"
            && Object.prototype.hasOwnProperty.call(current, "value")
            && !visited.has(current)
        ) {
            visited.add(current);
            current = current.value;
        }
        return current;
    }

    function normalizeOptionConfig(entry) {
        if (!entry || typeof entry !== "object" || !Array.isArray(entry.options)) return null;

        const options = entry.options.map((opt) => (typeof opt === "number" ? String(opt) : String(opt)));
        let value = unwrapValue(entry.value);
        if (value === undefined || value === null) value = "";
        if (typeof value === "number") value = String(value);

        return { value, options };
    }

    function normalizeConfigShape(cfg) {
        const source = (cfg && typeof cfg === "object") ? cfg : {};
        const normalized = {};

        Object.entries(source).forEach(([key, val]) => {
            const optionCfg = normalizeOptionConfig(val);
            normalized[key] = optionCfg || val;
        });

        return normalized;
    }

    function normalizeManageSteps(rawSteps) {
        return (rawSteps || []).map((step) => {
            const cfg = (step && step.Config) || {};
            const normalizedCfg = normalizeConfigShape(cfg);
            return { ...step, Config: normalizedCfg };
        });
    }

    function normalizeVisibilityMap(rawMap) {
        const normalized = {};
        if (!rawMap || typeof rawMap !== "object") return normalized;

        const asNumber = (val) => {
            if (Number.isFinite(val)) return val;
            const num = Number(val);
            return Number.isFinite(num) ? num : undefined;
        };

        Object.entries(rawMap).forEach(([scriptId, rows]) => {
            if (!Array.isArray(rows)) return;

            normalized[scriptId] = rows
                .map((row) => ({
                    config_key: row.config_key || row.ConfigKey || row.configKey,
                    client_visible: ("client_visible" in row)
                        ? row.client_visible
                        : (("clientVisible" in row) ? row.clientVisible : undefined),
                    client_label: row.client_label || row.clientLabel,
                    order_index: asNumber(row.order_index ?? row.orderIndex),
                }))
                .filter((row) => Boolean(row.config_key));
        });

        return normalized;
    }

    function getAccountButtons() {
        return accountsRoot
            ? Array.from(accountsRoot.querySelectorAll('[data-account-id]'))
            : [];
    }

    function applyAccountFilter(rawQuery) {
        if (!accountsRoot) return;
        const query = (rawQuery || "").trim().toLowerCase();
        let visibleCount = 0;

        getAccountButtons().forEach((btn) => {
            const haystack = [
                btn.dataset.accountName || "",
                btn.dataset.serverName || "",
                btn.dataset.ownerName || "",
            ].join(" ").toLowerCase();

            const matches = haystack.includes(query);
            const keepVisible = !query || matches;
            btn.classList.toggle("is-hidden", !keepVisible);
            if (keepVisible) visibleCount += 1;
        });

        if (accountsSearchEmpty) {
            accountsSearchEmpty.hidden = !query || visibleCount > 0;
        }
    }

    function prepareConfigPayload(payload, originalCfg = {}) {
        const result = {};

        Object.entries(payload || {}).forEach(([key, val]) => {
            const original = originalCfg ? originalCfg[key] : undefined;
            let next = unwrapValue(val);

            const shouldBeString = original
                && typeof original === "object"
                && Array.isArray(original.options);

            if (shouldBeString && (next === null || next === undefined)) {
                next = "";
            } else if (shouldBeString && typeof next === "number") {
                next = String(next);
            }

            result[key] = next;
        });

        return result;
    }

    function normalizeConfigValues(cfg) {
        if (!cfg || typeof cfg !== "object") {
            return {};
        }

        return prepareConfigPayload(cfg, cfg);
    }

    function resolveScheduleEditorUrl(stepIdx) {
        if (!state.selectedAccountId && state.selectedAccountId !== 0) return null;
        const menu = state.menu || {};
        const template = [
            menu.schedule_editor_url,
            menu.scheduleEditorUrl,
            menu.ScheduleEditorUrl,
            menu.schedule_editor,
        ].find((val) => typeof val === "string");

        if (!template) return null;
        if (template.includes("__ACCOUNT__") || template.includes("__STEP__")) {
            return replaceStepTemplate(template, state.selectedAccountId, stepIdx);
        }
        return template;
    }

    function openScheduleEditor(stepIdx) {
        openScheduleModal(stepIdx);
    }

    function getScriptTitle(step) {
        const cfg = (step && step.Config) || {};
        const scriptId = step && step.ScriptId;
        return cfg.Name || cfg.name || SCRIPT_LABELS[scriptId] || scriptId || "Шаг";
    }

    function getVisibilityForScript(scriptId) {
        if (!scriptId) return [];
        const map = state.visibilityMap || {};
        return map[scriptId] || [];
    }

    function visibilityIndexForScript(scriptId) {
        const index = {};
        getVisibilityForScript(scriptId).forEach((item) => {
            if (!item || !item.config_key) return;
            index[item.config_key] = item;
        });
        return index;
    }

    function findVisibilityProp(scriptId, prop) {
        const items = getVisibilityForScript(scriptId);
        const found = items.find((item) => item && item[prop]);
        return found ? found[prop] : undefined;
    }

    function isStepHidden(scriptId) {
        if (isAdminManage) return false;

        const items = getVisibilityForScript(scriptId) || [];
        return items.some((item) => item && item.config_key === STEP_HIDDEN_KEY && item.client_visible === false);
    }

    function findViewStepByRawIndex(rawIdx) {
        return (state.steps || []).find((step) => step && step.raw_index === rawIdx);
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
        const summaries = rules.map((rule) => {
            if (rule && typeof rule === "object" && typeof rule.Val1 === "string") {
                const [daysRaw, startRaw, endRaw] = rule.Val1.split("|").map((s) => (s || "").trim());
                const days = daysRaw ? daysRaw.split(",").map((d) => d.trim()).filter(Boolean) : null;
                const legacyRule = {
                    Days: days && days.length ? days : undefined,
                    StartAt: startRaw || undefined,
                    EndAt: endRaw || undefined,
                };
                return formatScheduleRule(legacyRule);
            }
            return formatScheduleRule(rule);
        }).filter(Boolean);
        if (summaries.length) return summaries.join("; ");
        if (rules.length) return `${rules.length} правил расписания`;
        return "";
    }

    function convertTimeTo24(val) {
        if (!val) return "";
        if (typeof val !== "string") return val;
        const normalized = val.trim();
        if (!normalized) return "";
        if (/am|pm/i.test(normalized)) {
            const parsed = new Date(`1970-01-01 ${normalized}`);
            if (!Number.isNaN(parsed.getTime())) {
                const hh = String(parsed.getHours()).padStart(2, "0");
                const mm = String(parsed.getMinutes()).padStart(2, "0");
                return `${hh}:${mm}`;
            }
        }
        return normalized;
    }

    function convertTimeTo12(val) {
        if (!val) return "";
        if (typeof val !== "string") return val;
        const [rawHours, rawMinutes = "00"] = val.split(":");
        const hours = Number(rawHours);
        const minutes = Number(rawMinutes);
        if (!Number.isFinite(hours)) return val;
        const suffix = hours >= 12 ? "PM" : "AM";
        const normalizedHours = ((hours + 11) % 12) + 1;
        const normalizedMinutes = Number.isFinite(minutes) ? String(minutes).padStart(2, "0") : "00";
        return `${normalizedHours}:${normalizedMinutes} ${suffix}`;
    }

    const DAY_OPTIONS = [
        { value: "mon", label: "Пн" },
        { value: "tue", label: "Вт" },
        { value: "wed", label: "Ср" },
        { value: "thu", label: "Чт" },
        { value: "fri", label: "Пт" },
        { value: "sat", label: "Сб" },
        { value: "sun", label: "Вс" },
    ];

    const DAY_ALIASES = {
        mon: "mon", monday: "mon", "понедельник": "mon", "пн": "mon",
        tue: "tue", tuesday: "tue", "вторник": "tue", "вт": "tue",
        wed: "wed", wednesday: "wed", "среда": "wed", "ср": "wed",
        thu: "thu", thursday: "thu", "четверг": "thu", "чт": "thu",
        fri: "fri", friday: "fri", "пятница": "fri", "пт": "fri",
        sat: "sat", saturday: "sat", "суббота": "sat", "сб": "sat",
        sun: "sun", sunday: "sun", "воскресенье": "sun", "вс": "sun",
    };

    function parseDaysValue(daysStr) {
        if (!daysStr || typeof daysStr !== "string") return [];
        const tokens = daysStr.split(/[,\s]+/).map((d) => d.trim().toLowerCase()).filter(Boolean);
        const mapped = tokens.map((token) => DAY_ALIASES[token] || token).filter(Boolean);
        return Array.from(new Set(mapped));
    }

    function stringifyDays(daysArr) {
        if (!Array.isArray(daysArr) || !daysArr.length) return "";
        return daysArr.join(", ");
    }

    function normalizeScheduleRule(rule) {
        const raw = rule || {};
        let days = raw.Days || raw.WeekDays || raw.Weekdays;
        let start = raw.StartAt || raw.Start || raw.From || raw.TimeFrom;
        let end = raw.EndAt || raw.End || raw.To || raw.TimeTo;
        const every = raw.Every || raw.Interval || raw.EveryMinutes;
        const label = raw.Label || raw.Name || "";
        const hasLegacy = typeof raw.Val1 === "string";

        if (hasLegacy) {
            const [daysRaw, startRaw, endRaw] = raw.Val1.split("|").map((s) => (s || "").trim());
            if (!days) days = daysRaw ? daysRaw.split(",").map((d) => d.trim()).filter(Boolean) : "";
            if (!start) start = startRaw;
            if (!end) end = endRaw;
        }

        const daysStr = Array.isArray(days) ? days.join(", ") : (days || "");

        return {
            days: daysStr,
            start: convertTimeTo24(start),
            end: convertTimeTo24(end),
            every: every ?? "",
            label: label || "",
            raw,
        };
    }

    function buildScheduleRulePayload(rawRule, draft) {
        const base = rawRule && typeof rawRule === "object" ? { ...rawRule } : {};
        const daysArr = draft.days ? parseDaysValue(draft.days) : [];
        const start = draft.start || "";
        const end = draft.end || "";
        const every = draft.every === "" ? "" : draft.every;
        const label = draft.label || "";

        if (daysArr.length) {
            base.Days = daysArr;
        } else {
            delete base.Days;
            delete base.WeekDays;
            delete base.Weekdays;
        }

        if (start) {
            base.StartAt = start;
            base.Start = start;
            base.From = start;
            base.TimeFrom = start;
        } else {
            delete base.StartAt;
            delete base.Start;
            delete base.From;
            delete base.TimeFrom;
        }

        if (end) {
            base.EndAt = end;
            base.End = end;
            base.To = end;
            base.TimeTo = end;
        } else {
            delete base.EndAt;
            delete base.End;
            delete base.To;
            delete base.TimeTo;
        }

        if (every !== "") {
            base.Every = every;
            base.Interval = every;
            base.EveryMinutes = every;
        } else {
            delete base.Every;
            delete base.Interval;
            delete base.EveryMinutes;
        }

        if (label) {
            base.Label = label;
            base.Name = base.Name || label;
        } else {
            delete base.Label;
        }

        const val1Days = daysArr.join(",");
        const val1Start = convertTimeTo12(start) || start || "";
        const val1End = convertTimeTo12(end) || end || "";
        base.Val1 = `${val1Days}|${val1Start}|${val1End}`;

        return base;
    }

    function renderDaysPicker(triggerEl, row, inputEl) {
        if (!triggerEl || !row || !inputEl) return;
        const existing = document.querySelector(".schedule-days-modal");
        if (existing) existing.remove();

        const selected = Array.isArray(row._selectedDays)
            ? [...row._selectedDays]
            : parseDaysValue(inputEl.value || "");

        const modal = document.createElement("div");
        modal.className = "schedule-days-modal is-open";
        modal.innerHTML = `
            <div class="schedule-days-modal__backdrop" data-role="days-close"></div>
            <div class="schedule-days-modal__dialog" role="dialog" aria-modal="true">
                <div class="schedule-days-modal__header">
                    <div>
                        <div class="schedule-days-modal__title">Дни недели</div>
                        <div class="schedule-days-modal__subtitle">Отметьте нужные дни или выберите все</div>
                    </div>
                    <button type="button" class="schedule-days-modal__close" data-role="days-close" aria-label="Закрыть">×</button>
                </div>
                <div class="schedule-days-modal__body" data-role="days-list"></div>
                <div class="schedule-days-modal__footer">
                    <label class="schedule-days-modal__select-all">
                        <input type="checkbox" data-role="days-select-all">
                        <span>Выбрать все дни</span>
                    </label>
                    <div class="schedule-days-modal__actions">
                        <button type="button" class="btn btn-secondary btn-small" data-role="days-close">Отмена</button>
                        <button type="button" class="btn btn-primary btn-small" data-role="days-apply">Применить</button>
                    </div>
                </div>
            </div>
        `;

        const list = modal.querySelector('[data-role="days-list"]');
        if (list) {
            DAY_OPTIONS.forEach((opt) => {
                const id = `day-${opt.value}-${Math.random().toString(36).slice(2, 8)}`;
                const wrapper = document.createElement("label");
                wrapper.className = "schedule-days-modal__item";
                wrapper.innerHTML = `
                    <input type="checkbox" value="${opt.value}" id="${id}" ${selected.includes(opt.value) ? "checked" : ""}>
                    <span>${opt.label}</span>
                `;
                list.appendChild(wrapper);
            });
        }

        const selectAll = modal.querySelector('[data-role="days-select-all"]');
        const applyBtn = modal.querySelector('[data-role="days-apply"]');

        const syncSelectAll = () => {
            if (!selectAll) return;
            const checkboxes = Array.from(modal.querySelectorAll('[data-role="days-list"] input[type="checkbox"]'));
            if (!checkboxes.length) return;
            selectAll.checked = checkboxes.every((cb) => cb.checked);
        };

        if (selectAll) {
            selectAll.addEventListener("change", () => {
                const checkboxes = modal.querySelectorAll('[data-role="days-list"] input[type="checkbox"]');
                checkboxes.forEach((cb) => { cb.checked = selectAll.checked; });
            });
        }

        modal.addEventListener("click", (event) => {
            if (event.target.closest('[data-role="days-close"]')) {
                modal.remove();
            }
        });

        if (applyBtn) {
            applyBtn.addEventListener("click", () => {
                const chosen = Array.from(modal.querySelectorAll('[data-role="days-list"] input[type="checkbox"]'))
                    .filter((cb) => cb.checked)
                    .map((cb) => cb.value);
                row._selectedDays = chosen;
                inputEl.value = stringifyDays(chosen);
                inputEl.dispatchEvent(new Event("input", { bubbles: true }));
                modal.remove();
            });
        }

        modal.addEventListener("change", (event) => {
            if (event.target.closest('[data-role="days-list"]')) {
                syncSelectAll();
            }
        });

        document.body.appendChild(modal);
        syncSelectAll();
    }

    function createScheduleEditor(step) {
        if (!isAdminManage) return null;
        const rules = Array.isArray(step && step.ScheduleRules) ? step.ScheduleRules : [];

        const container = document.createElement("div");
        container.className = "config-schedule";
        container.dataset.role = "schedule-editor";
        container.innerHTML = `
            <div class="config-schedule__header">
                <div>
                    <div class="config-schedule__title">Таймеры</div>
                    <div class="config-schedule__subtitle">Настройте дни, интервал и время запуска шага</div>
                </div>
                <button type="button" class="btn btn-secondary btn-small" data-role="schedule-add">Добавить правило</button>
            </div>
            <div class="config-schedule__list" data-role="schedule-list"></div>
        `;

        const list = container.querySelector('[data-role="schedule-list"]');

        const updateEmptyState = () => {
            if (!list) return;
            const hasRows = list.querySelector('[data-role="schedule-row"]');
            if (!hasRows) {
                list.innerHTML = '<div class="config-empty">Расписание не настроено.</div>';
            }
        };

        const createRow = (rule) => {
            const normalized = normalizeScheduleRule(rule);
            const row = document.createElement("div");
            row.className = "config-schedule__row";
            row.dataset.role = "schedule-row";
            row._rawRule = rule || {};
            row._selectedDays = parseDaysValue(normalized.days);
            row.innerHTML = `
                <label class="config-schedule__field">
                    <span>Дни</span>
                    <div class="config-schedule__days-input">
                        <input type="text" data-schedule-field="days" value="${escapeHtml(stringifyDays(parseDaysValue(normalized.days)) || normalized.days)}" placeholder="mon,tue,wed" readonly>
                        <button type="button" class="config-schedule__days-trigger" data-role="schedule-days-trigger" aria-label="Выбрать дни недели">📅</button>
                    </div>
                </label>
                <label class="config-schedule__field">
                    <span>Начало</span>
                    <input type="time" data-schedule-field="start" value="${escapeHtml(normalized.start)}" placeholder="08:00">
                </label>
                <label class="config-schedule__field">
                    <span>Конец</span>
                    <input type="time" data-schedule-field="end" value="${escapeHtml(normalized.end)}" placeholder="23:00">
                </label>
                <label class="config-schedule__field">
                    <span>Интервал (мин)</span>
                    <input type="text" data-schedule-field="every" value="${escapeHtml(normalized.every)}" placeholder="60">
                </label>
                <label class="config-schedule__field">
                    <span>Название</span>
                    <input type="text" data-schedule-field="label" value="${escapeHtml(normalized.label)}" placeholder="Ночь / фарм / ...">
                </label>
                <button type="button" class="btn btn-secondary btn-small config-schedule__remove" data-role="schedule-remove">Удалить</button>
            `;

            const removeBtn = row.querySelector('[data-role="schedule-remove"]');
            if (removeBtn) {
                removeBtn.addEventListener("click", () => {
                    row.remove();
                    updateEmptyState();
                });
            }

            const daysTrigger = row.querySelector('[data-role="schedule-days-trigger"]');
            const daysInput = row.querySelector('[data-schedule-field="days"]');
            if (daysTrigger && daysInput) {
                const openPicker = (event) => {
                    event.preventDefault();
                    renderDaysPicker(daysTrigger, row, daysInput);
                };
                daysTrigger.addEventListener("click", openPicker);
                daysInput.addEventListener("click", openPicker);
            }

            return row;
        };

        rules.forEach((rule) => {
            const row = createRow(rule);
            if (row && list) list.appendChild(row);
        });

        updateEmptyState();

        const addBtn = container.querySelector('[data-role="schedule-add"]');
        if (addBtn && list) {
            addBtn.addEventListener("click", () => {
                const row = createRow({});
                list.appendChild(row);
            });
        }

        return container;
    }

    function ensureScheduleModalStyles() {
        if (document.getElementById("schedule-modal-styles")) return;
        const style = document.createElement("style");
        style.id = "schedule-modal-styles";
        style.textContent = `
            .schedule-modal { position: fixed; inset: 0; display: none; align-items: center; justify-content: center; z-index: 2000; }
            .schedule-modal.is-open { display: flex; }
            .schedule-modal__backdrop { position: absolute; inset: 0; background: rgba(0,0,0,0.45); }
            .schedule-modal__dialog { position: relative; background: #0f172a; color: #e2e8f0; border-radius: 12px; padding: 20px; box-shadow: 0 20px 60px rgba(0,0,0,0.35); width: min(900px, 96vw); max-height: 90vh; overflow: hidden; display: flex; flex-direction: column; gap: 12px; }
            .schedule-modal__header { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
            .schedule-modal__title { font-size: 20px; font-weight: 700; }
            .schedule-modal__subtitle { color: #94a3b8; font-size: 14px; margin-top: 4px; }
            .schedule-modal__body { overflow: auto; padding-right: 4px; }
            .schedule-modal__footer { display: flex; justify-content: flex-end; gap: 10px; }
            .schedule-modal__close { background: none; border: none; color: #94a3b8; font-size: 20px; cursor: pointer; }
            .schedule-days-modal { position: fixed; inset: 0; display: none; align-items: center; justify-content: center; z-index: 2100; }
            .schedule-days-modal.is-open { display: flex; }
            .schedule-days-modal__backdrop { position: absolute; inset: 0; background: rgba(0,0,0,0.45); }
            .schedule-days-modal__dialog { position: relative; background: #0f172a; color: #e2e8f0; border-radius: 12px; padding: 16px; box-shadow: 0 20px 60px rgba(0,0,0,0.35); width: min(460px, 94vw); display: flex; flex-direction: column; gap: 12px; }
            .schedule-days-modal__header { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
            .schedule-days-modal__title { font-size: 18px; font-weight: 700; }
            .schedule-days-modal__subtitle { color: #94a3b8; font-size: 13px; margin-top: 4px; }
            .schedule-days-modal__close { background: none; border: none; color: #94a3b8; font-size: 20px; cursor: pointer; }
            .schedule-days-modal__body { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 8px; }
            .schedule-days-modal__item { display: flex; align-items: center; gap: 8px; padding: 8px; background: #111827; border-radius: 8px; border: 1px solid #1e293b; cursor: pointer; }
            .schedule-days-modal__item input { accent-color: #38bdf8; }
            .schedule-days-modal__footer { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
            .schedule-days-modal__actions { display: flex; gap: 8px; }
            .schedule-days-modal__select-all { display: flex; align-items: center; gap: 8px; color: #e2e8f0; }
            .config-schedule__days-input { display: flex; align-items: center; gap: 6px; }
            .config-schedule__days-input input[readonly] { cursor: pointer; background: #0b1224; border: 1px solid #1e293b; border-radius: 8px; padding-right: 36px; }
            .config-schedule__days-trigger { min-width: 32px; height: 32px; border-radius: 8px; border: 1px solid #1e293b; background: #111827; color: #e2e8f0; cursor: pointer; }
            .config-schedule__days-trigger:hover { background: #1f2937; }
        `;
        document.head.appendChild(style);
    }

    function closeScheduleModal(modal) {
        if (!modal) return;
        modal.classList.remove("is-open");
    }

    function openScheduleModal(stepIdx) {
        if (!isAdminManage) return;
        ensureScheduleModalStyles();
        const existing = document.querySelector(`.schedule-modal[data-step-idx="${stepIdx}"]`);
        const modal = existing || document.createElement("div");
        modal.className = "schedule-modal";
        modal.dataset.role = "schedule-modal";
        modal.dataset.stepIdx = String(stepIdx);
        modal.innerHTML = `
            <div class="schedule-modal__backdrop" data-role="schedule-modal-close"></div>
            <div class="schedule-modal__dialog" role="dialog" aria-modal="true">
                <div class="schedule-modal__header">
                    <div>
                        <div class="schedule-modal__title">Настройка таймеров</div>
                        <div class="schedule-modal__subtitle">Укажите дни, временные рамки и интервал срабатывания шага</div>
                    </div>
                    <button type="button" class="schedule-modal__close" data-role="schedule-modal-close" aria-label="Закрыть">×</button>
                </div>
                <div class="schedule-modal__body" data-role="schedule-modal-body"></div>
                <div class="schedule-modal__footer">
                    <button type="button" class="btn btn-secondary btn-small" data-role="schedule-modal-close">Отмена</button>
                    <button type="button" class="btn btn-primary btn-small" data-role="schedule-modal-save">Сохранить таймеры</button>
                </div>
            </div>
        `;

        const body = modal.querySelector('[data-role="schedule-modal-body"]');
        if (body) {
            body.innerHTML = "";
            const rawStep = state.rawSteps[stepIdx] || {};
            const draft = (state.scheduleDrafts && Object.prototype.hasOwnProperty.call(state.scheduleDrafts, stepIdx))
                ? state.scheduleDrafts[stepIdx]
                : undefined;
            const stepData = draft !== undefined ? { ...rawStep, ScheduleRules: draft } : rawStep;
            const editor = createScheduleEditor(stepData);
            if (editor) {
                editor.dataset.stepIdx = String(stepIdx);
                body.appendChild(editor);
            }
        }

        if (!existing) {
            modal.addEventListener("click", (event) => {
                if (event.target.closest('[data-role="schedule-modal-close"]')) {
                    closeScheduleModal(modal);
                }
            });

            const saveBtn = modal.querySelector('[data-role="schedule-modal-save"]');
            if (saveBtn) {
                saveBtn.addEventListener("click", () => {
                    const rules = collectScheduleRules(modal, state.rawSteps[stepIdx] && state.rawSteps[stepIdx].ScheduleRules, { ignoreDraft: true });
                    state.scheduleDrafts[stepIdx] = rules;
                    closeScheduleModal(modal);

                    if (Number(state.selectedStepIndex) === Number(stepIdx) && currentConfigForm) {
                        saveConfig(stepIdx, currentConfigForm, state.rawSteps[stepIdx] && state.rawSteps[stepIdx].Config);
                    }
                });
            }

            document.body.appendChild(modal);
        }

        modal.classList.add("is-open");
    }

    function renderScheduleEditor(rootEl, step) {
        if (!rootEl || !isAdminManage) return;

        const stepIdx = Number(rootEl.dataset ? rootEl.dataset.stepIdx : state.selectedStepIndex);
        const hasDraft = Number.isFinite(stepIdx)
            && state.scheduleDrafts
            && Object.prototype.hasOwnProperty.call(state.scheduleDrafts, stepIdx);
        const stepData = hasDraft ? { ...step, ScheduleRules: state.scheduleDrafts[stepIdx] } : step;

        const editor = createScheduleEditor(stepData);
        if (editor) {
            editor.dataset.stepIdx = String(Number.isFinite(stepIdx) ? stepIdx : state.selectedStepIndex || 0);
            rootEl.appendChild(editor);
        }
    }

    function buildViewStepsFromRaw(rawSteps, visibilityMap) {
        state.visibilityMap = visibilityMap || state.visibilityMap || {};

        return (rawSteps || [])
            .map((step, idx) => {
                const cfg = (step && step.Config) || {};
                const scriptId = step && step.ScriptId;
                const description = cfg.Description || cfg.description || "";
                const schedule_rules = (step && Array.isArray(step.ScheduleRules)) ? step.ScheduleRules : [];

                if (isStepHidden(scriptId, step)) {
                    return null;
                }

                const nameOverride = findVisibilityProp(scriptId, "client_label");
                const name = nameOverride || getScriptTitle(step) || `Шаг ${idx + 1}`;

                return {
                    index: idx,
                    raw_index: idx,
                    name,
                    script_id: scriptId,
                    config: cfg,
                    description,
                    is_active: step && typeof step.IsActive === "boolean" ? step.IsActive : true,
                    schedule_summary: scheduleSummary(null, step) || undefined,
                    schedule_rules_count: schedule_rules.length,
                };
            })
            .filter(Boolean);
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

        updateDefaultsButtonState();
        updateCopyOpenState();
    }

    function updateDefaultsButtonState() {
        if (!defaultsToolbarBtn) return;
        const hasAccount = Boolean(state.selectedAccountId);
        const canUseDefaults = hasAccount && state.selectedAccountHasDefaults && !applyDefaultsInProgress;

        defaultsToolbarBtn.hidden = !hasAccount;
        defaultsToolbarBtn.disabled = !canUseDefaults;

        if (hasAccount && !state.selectedAccountHasDefaults) {
            defaultsToolbarBtn.title = "Для этого тарифа нет схемы по умолчанию.";
        } else {
            defaultsToolbarBtn.removeAttribute("title");
        }
    }

    function openDefaultsConfirm() {
        if (defaultsToolbarBtn && defaultsToolbarBtn.disabled) return;
        if (!state.selectedAccountId || !state.selectedAccountHasDefaults || applyDefaultsInProgress) return;
        if (!defaultsModal) {
            applyDefaultsForCurrentAccount(defaultsToolbarBtn);
            return;
        }

        defaultsModal.hidden = false;
        requestAnimationFrame(() => defaultsModal.classList.add("is-visible"));
        document.body.classList.add("has-modal");
    }

    function closeDefaultsConfirm() {
        if (!defaultsModal) return;
        defaultsModal.classList.remove("is-visible");
        document.body.classList.remove("has-modal");
        setTimeout(() => {
            if (!defaultsModal.classList.contains("is-visible")) {
                defaultsModal.hidden = true;
            }
        }, 160);
    }

    function setCopyStatus(message = "", status = "") {
        if (!copyStatus) return;
        copyStatus.textContent = message;
        copyStatus.classList.remove("is-error", "is-success", "is-warning");
        if (status) {
            copyStatus.classList.add(`is-${status}`);
        }
    }

    function getAccountById(accountId) {
        return adminAccounts.find((acc) => Number(acc.id) === Number(accountId));
    }

    function formatAccountLabel(account) {
        const name = account && account.name ? account.name : "—";
        const server = account && account.server ? account.server : "N/A";
        const owner = account && account.owner ? account.owner : "—";
        return `${name} · ${server} · ${owner}`;
    }

    function updateCopyOpenState() {
        if (!copyOpenBtn) return;
        copyOpenBtn.disabled = adminAccounts.length < 2;
    }

    function getCopyMode() {
        if (!state.copyMode) {
            state.copyMode = "same-server";
        }
        return state.copyMode;
    }

    function setCopyMode(mode) {
        state.copyMode = mode || "same-server";
        renderCopyTabs();
        renderCopyNotes();
        renderTargetOptions();
        updateTargetsAllState();
        if (copyConfirmInput) {
            copyConfirmInput.checked = false;
        }
        setCopyStatus("");
        updateCopyConfirmState();
    }

    function renderCopyTabs() {
        if (!copyTabs.length) return;
        const mode = getCopyMode();
        copyTabs.forEach((btn) => {
            btn.classList.toggle("is-active", btn.dataset.copyMode === mode);
        });
    }

    function renderCopyNotes() {
        const mode = getCopyMode();
        if (copySubtitle) {
            copySubtitle.textContent = mode === "cross-server"
                ? "Скопируйте настройки между фермами разных серверов."
                : "Скопируйте настройки 1-в-1 между фермами одного сервера.";
        }
        if (copyNoteSameServer) {
            copyNoteSameServer.hidden = mode !== "same-server";
        }
        if (copyNoteCrossServer) {
            copyNoteCrossServer.hidden = mode !== "cross-server";
        }
    }

    function renderSourceOptions() {
        if (!copySourceSelect) return;
        const accounts = adminAccounts.slice().sort((a, b) => {
            return (a.name || "").localeCompare((b.name || ""), "ru");
        });
        copySourceSelect.innerHTML = "";

        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = accounts.length ? "Выберите ферму" : "Нет доступных ферм";
        placeholder.selected = true;
        copySourceSelect.appendChild(placeholder);

        accounts.forEach((acc) => {
            const option = document.createElement("option");
            option.value = String(acc.id);
            option.textContent = formatAccountLabel(acc);
            copySourceSelect.appendChild(option);
        });

        if (state.selectedAccountId) {
            copySourceSelect.value = String(state.selectedAccountId);
        }
    }

    function updateCopySourceMeta() {
        if (!copySourceMeta || !copySourceSelect) return;
        const sourceId = Number(copySourceSelect.value);
        const account = getAccountById(sourceId);
        if (!account) {
            copySourceMeta.textContent = "";
            return;
        }
        const status = account.is_active ? "активна" : "неактивна";
        copySourceMeta.textContent = `${account.server || "N/A"} · ${account.owner || "—"} · ${status}`;
    }

    function renderTargetOptions() {
        if (!copyTargetsWrap || !copySourceSelect) return;
        const sourceId = Number(copySourceSelect.value);
        const sourceAccount = getAccountById(sourceId);
        const sourceServerId = sourceAccount ? sourceAccount.server_id : null;
        const mode = getCopyMode();
        const targets = adminAccounts
            .filter((acc) => Number(acc.id) !== sourceId)
            .filter((acc) => {
                if (mode === "cross-server") {
                    return true;
                }
                return sourceServerId === null || acc.server_id === sourceServerId;
            })
            .sort((a, b) => (a.name || "").localeCompare((b.name || ""), "ru"));

        copyTargetsWrap.innerHTML = "";
        targets.forEach((acc) => {
            const label = document.createElement("label");
            label.className = "copy-settings-modal__target-item";
            label.innerHTML = `
                <input type="checkbox" value="${acc.id}">
                <span>${formatAccountLabel(acc)}</span>
            `;
            copyTargetsWrap.appendChild(label);
        });

        if (copyTargetsHint) {
            if (!sourceAccount) {
                copyTargetsHint.textContent = "Выберите источник, чтобы увидеть доступные цели.";
            } else if (!targets.length) {
                copyTargetsHint.textContent = mode === "cross-server"
                    ? "Нет других ферм для копирования."
                    : "На этом сервере нет других ферм для копирования.";
            } else {
                copyTargetsHint.textContent = "";
            }
        }

        if (copyTargetsAll) {
            copyTargetsAll.checked = false;
            copyTargetsAll.disabled = !targets.length;
        }
    }

    function collectSelectedTargetIds() {
        if (!copyTargetsWrap) return [];
        return Array.from(copyTargetsWrap.querySelectorAll("input[type='checkbox']:checked"))
            .map((el) => Number(el.value))
            .filter((id) => !Number.isNaN(id));
    }

    function updateTargetsAllState() {
        if (!copyTargetsAll || !copyTargetsWrap) return;
        const boxes = Array.from(copyTargetsWrap.querySelectorAll("input[type='checkbox']"));
        if (!boxes.length) {
            copyTargetsAll.checked = false;
            copyTargetsAll.disabled = true;
            return;
        }
        const checked = boxes.filter((el) => el.checked).length;
        copyTargetsAll.checked = checked > 0 && checked === boxes.length;
        copyTargetsAll.disabled = false;
    }

    function toggleTargetsAll() {
        if (!copyTargetsAll || !copyTargetsWrap) return;
        const boxes = Array.from(copyTargetsWrap.querySelectorAll("input[type='checkbox']"));
        boxes.forEach((el) => {
            el.checked = copyTargetsAll.checked;
        });
        updateCopyConfirmState();
    }

    function updateCopyConfirmState() {
        if (!copyConfirmBtn || !copySourceSelect || !copyConfirmInput) return;
        const hasSource = Boolean(copySourceSelect.value);
        const confirmed = Boolean(copyConfirmInput.checked);
        const selectedTargets = collectSelectedTargetIds();
        copyConfirmBtn.disabled = !(hasSource && confirmed && selectedTargets.length);
    }

    function openCopyModal() {
        if (!copyModal) return;
        getCopyMode();
        renderCopyTabs();
        renderCopyNotes();
        renderSourceOptions();
        updateCopySourceMeta();
        renderTargetOptions();
        updateTargetsAllState();
        setCopyStatus("");
        if (copyConfirmInput) copyConfirmInput.checked = false;
        updateCopyConfirmState();

        copyModal.hidden = false;
        requestAnimationFrame(() => copyModal.classList.add("is-open"));
        document.body.classList.add("has-modal");
    }

    function closeCopyModal() {
        if (!copyModal) return;
        copyModal.classList.remove("is-open");
        document.body.classList.remove("has-modal");
        setTimeout(() => {
            if (!copyModal.classList.contains("is-open")) {
                copyModal.hidden = true;
            }
        }, 160);
    }

    async function applyCopySettings() {
        if (!copySourceSelect) return;
        const sourceId = Number(copySourceSelect.value);
        const targetIds = collectSelectedTargetIds();
        if (!sourceId) {
            setCopyStatus("Выберите аккаунт-источник.", "error");
            return;
        }
        if (!targetIds.length) {
            setCopyStatus("Выберите хотя бы одну целевую ферму.", "error");
            return;
        }

        const originalText = copyConfirmBtn ? copyConfirmBtn.textContent : "";
        if (copyConfirmBtn) {
            copyConfirmBtn.disabled = true;
            copyConfirmBtn.textContent = "Копируем...";
        }
        setCopyStatus("");

        try {
            const url = state.copySettingsUrlTemplate;
            const resp = await fetch(url, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    source_account_id: sourceId,
                    target_account_ids: targetIds,
                    allow_cross_server: getCopyMode() === "cross-server",
                }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) {
                throw new Error(data.error || "Не удалось копировать настройки");
            }

            const copiedCount = typeof data.copied_accounts === "number" ? data.copied_accounts : 0;
            setCopyStatus(`Настройки скопированы для ${copiedCount} фермы(м).`, "success");
            showMiniToast("Настройки скопированы", "success");
        } catch (err) {
            console.error(err);
            setCopyStatus(err.message || "Не удалось копировать настройки", "error");
        } finally {
            if (copyConfirmBtn) {
                copyConfirmBtn.textContent = originalText || "Копировать";
            }
            updateCopyConfirmState();
        }
    }


    function renderSteps() {
    // Если контейнер для шагов не найден — выходим
    if (!stepsRoot) return;

    // Если ещё не выбрана ферма
    if (!state.selectedAccountId) {
        renderEmptyState("Выберите ферму слева, чтобы увидеть настройки.");
        return;
    }

    // Если по выбранной ферме нет шагов
    if (!state.rawSteps || !state.rawSteps.length) {
        renderEmptyState("Шаги для этой фермы не найдены.", state.debugInfo);
        return;
    }

    // Рендерим карточки шагов
    const html = (state.steps || []).map((viewStep) => {
        const rawStep = state.rawSteps[viewStep.raw_index] || {};
        const desc = viewStep.description
            ? `<div class="step-desc">${viewStep.description}</div>`
            : "";

        const schedule = scheduleSummary(viewStep, rawStep);
        const scheduleSummaryHtml = (isAdminManage && schedule)
            ? `<span class="step-schedule__summary">⏱ ${schedule}</span>`
            : "";
        const scheduleHtml = scheduleSummaryHtml
            ? `<div class="step-schedule">${scheduleSummaryHtml}</div>`
            : "";

        const switchId = `step-toggle-${state.selectedAccountId || "acc"}-${viewStep.raw_index}`;
        const isSelected = state.selectedStepIndex === viewStep.raw_index;
        const name = getScriptTitle(rawStep) || viewStep.name || `Шаг ${viewStep.index + 1}`;

        // Кнопка редактирования расписания отображается только в админ-версии
        const scheduleButtonHtml = isAdminManage
            ? [
                '<div class="step-actions__schedule">',
                `    <button class="step-schedule__edit" type="button" data-role="schedule-edit" data-step-idx="${viewStep.raw_index}" aria-label="Редактировать расписание шага">⏲</button>`,
                '</div>',
            ].join("\n")
            : "";

        return `
            <div class="manage-step-card ${isSelected ? "is-selected" : ""}" data-step-idx="${viewStep.raw_index}">
                <div class="manage-step-head">
                    <div>
                        <div class="step-title">${name}</div>
                        ${desc}
                        ${scheduleHtml}
                    </div>
                    <div class="step-actions">
                        <label class="ios-switch" for="${switchId}">
                            <input
                                type="checkbox"
                                id="${switchId}"
                                class="ios-switch__input"
                                data-role="step-toggle"
                                data-account-id="${state.selectedAccountId}"
                                data-step-idx="${viewStep.raw_index}"
                                ${viewStep.is_active ? "checked" : ""}
                            >
                            <span class="ios-switch__slider" aria-hidden="true"></span>
                        </label>
                        ${scheduleButtonHtml}
                    </div>
                </div>
            </div>
        `;
    }).join("");

    // Вставляем HTML в DOM
    stepsRoot.innerHTML = html;

    // Обновляем заголовки справа / сверху
    updateHeaderText();
}


    let currentConfigForm = null;
    let configAutoSaveTimer = null;
    let configSaveInProgress = false;
    let configAutoSaveQueued = false;
    let lastConfigToastAt = 0;
    let applyDefaultsInProgress = false;

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
        if (isStepHidden(step && step.ScriptId, step)) {
            configRoot.innerHTML = '<div class="config-empty">Этот таймер скрыт.</div>';
            updateHeaderText();
            return;
        }
        const cfg = step.Config || {};
        const title = getScriptTitle(step);
        const subtitle = state.selectedAccountName || "";
        const visibilityIndex = visibilityIndexForScript(step.ScriptId);

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

        const actions = document.createElement("div");
        actions.className = "config-header__actions";

        const tariffBadge = document.createElement("div");
        tariffBadge.className = "config-tariff";
        const hasTariffName = !!state.selectedAccountTariffName;
        const hasTariffPrice = state.selectedAccountTariffPrice !== null && state.selectedAccountTariffPrice !== undefined;

        if (hasTariffName && hasTariffPrice) {
            tariffBadge.textContent = `Тариф: ${state.selectedAccountTariffName} (${state.selectedAccountTariffPrice} ₽)`;
        } else if (hasTariffName) {
            tariffBadge.textContent = `Тариф: ${state.selectedAccountTariffName}`;
        } else if (hasTariffPrice) {
            tariffBadge.textContent = `Тариф: ${state.selectedAccountTariffPrice} ₽`;
        }

        if (tariffBadge.textContent) {
            actions.appendChild(tariffBadge);
        }
        header.appendChild(actions);
        configRoot.appendChild(header);

        const form = document.createElement("div");
        form.className = "config-form";

        const orderedKeys = (() => {
            const keys = Object.keys(cfg || {});
            const preferred = ORDER_MAP[step.ScriptId] || [];
            const visibleKeys = keys.filter((k) => {
                const rule = visibilityIndex[k];
                return !(rule && rule.client_visible === false);
            });

            const orderOf = (key) => {
                const rule = visibilityIndex[key];
                return (rule && Number.isFinite(rule.order_index)) ? rule.order_index : null;
            };

            const preferredIndex = (key) => {
                const idx = preferred.indexOf(key);
                return idx === -1 ? null : idx;
            };

            return visibleKeys.sort((a, b) => {
                const orderA = orderOf(a);
                const orderB = orderOf(b);
                if (orderA !== null || orderB !== null) {
                    const safeA = orderA !== null ? orderA : Number.POSITIVE_INFINITY;
                    const safeB = orderB !== null ? orderB : Number.POSITIVE_INFINITY;
                    if (safeA !== safeB) return safeA - safeB;
                }

                const prefA = preferredIndex(a);
                const prefB = preferredIndex(b);
                if (prefA !== null || prefB !== null) {
                    const safePrefA = prefA !== null ? prefA : Number.POSITIVE_INFINITY;
                    const safePrefB = prefB !== null ? prefB : Number.POSITIVE_INFINITY;
                    if (safePrefA !== safePrefB) return safePrefA - safePrefB;
                }

                return a.localeCompare(b, "ru");
            });
        })();

        if (!orderedKeys.length) {
            form.innerHTML = '<div class="config-empty">Нет параметров для этого шага.</div>';
        }

        orderedKeys.forEach((key) => {
            const conf = cfg[key];
            const field = document.createElement("div");
            field.className = "config-field";
            const visibilityRule = visibilityIndex[key];
            const label = (visibilityRule && visibilityRule.client_label) || CONFIG_LABELS[key] || key;

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

        renderScheduleEditor(form, step);

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
            if (isActive) {
                updatePaymentWarning(btn);
            }
        });
    }

    function applyAccountActiveState(btn, isActive, paymentBlockedOverride) {
        if (!btn) return;
        btn.dataset.accountActive = isActive ? "1" : "0";
        if (typeof paymentBlockedOverride === "boolean") {
            btn.dataset.paymentBlocked = paymentBlockedOverride ? "1" : "0";
        }
        const paymentBlocked = btn.dataset.paymentBlocked === "1";

        btn.classList.toggle("is-disabled", !isActive);
        btn.classList.toggle("is-payment-blocked", paymentBlocked);
        const toggle = btn.querySelector('input[data-role="account-toggle"]');
        if (toggle) {
            toggle.checked = isActive;
            toggle.dataset.activeState = isActive ? "1" : "0";
            toggle.dataset.paymentBlocked = paymentBlocked ? "1" : "0";
            toggle.disabled = paymentBlocked && !isAdminManage;
        }
    }

    function updatePaymentWarning(selectedBtn) {
        if (!paymentWarning) return;
        const isBlocked = !!(selectedBtn && selectedBtn.dataset.paymentBlocked === "1");
        paymentWarning.hidden = !isBlocked;
    }

    function syncAccountsUi() {
        if (!accountsRoot) return;
        accountsRoot.querySelectorAll('[data-account-id]').forEach((btn) => {
            const isActive = btn.dataset.accountActive !== "0";
            const paymentBlocked = btn.dataset.paymentBlocked === "1";
            applyAccountActiveState(btn, isActive, paymentBlocked);
        });
        const current = accountsRoot.querySelector('.manage-modern__account.is-active')
            || accountsRoot.querySelector('[data-account-id]');
        updatePaymentWarning(current);
    }

    async function toggleStep(accountId, stepIdx, nextActive, control) {
        if (!accountId) return;
        const desiredState = Boolean(nextActive);
        if (control) control.disabled = true;
        try {
            const url = replaceStepTemplate(state.toggleUrlTemplate, accountId, stepIdx);
            const resp = await fetch(url, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ is_active: desiredState }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) throw new Error(data.error || "Ошибка сохранения");
            const viewStep = findViewStepByRawIndex(stepIdx);
            if (viewStep) viewStep.is_active = desiredState;
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

    async function toggleAccountActive(accountId, nextActive, control) {
        if (!accountId) return;
        const desiredState = Boolean(nextActive);
        const prevState = control ? control.dataset.activeState === "1" : null;
        const paymentBlocked = control ? control.dataset.paymentBlocked === "1" : false;
        if (paymentBlocked && !isAdminManage && desiredState) {
            if (control && prevState !== null) {
                control.checked = prevState;
            }
            alert("Ферма отключена до оплаты. Напишите @tvoinetakoi, чтобы возобновить работу.");
            return;
        }
        if (control) control.disabled = true;
        try {
            const url = replaceTemplate(state.accountToggleUrlTemplate, accountId);
            const resp = await fetch(url, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ is_active: desiredState }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) throw new Error(data.error || "Не удалось обновить ферму");
            const btn = accountsRoot && accountsRoot.querySelector(`[data-account-id="${accountId}"]`);
            const blockedFlag = typeof data.blocked_for_payment === "boolean"
                ? data.blocked_for_payment
                : (btn ? btn.dataset.paymentBlocked === "1" : false);
            if (btn) {
                btn.dataset.paymentBlocked = blockedFlag ? "1" : "0";
            }
            applyAccountActiveState(btn, desiredState, blockedFlag);
            if (control) control.dataset.paymentBlocked = blockedFlag ? "1" : "0";
            if (btn && String(state.selectedAccountId) === String(accountId)) {
                updatePaymentWarning(btn);
            }
        } catch (err) {
            console.error(err);
            if (control && prevState !== null) {
                control.checked = prevState;
                control.dataset.activeState = prevState ? "1" : "0";
            }
            alert(err.message || "Не удалось обновить ферму");
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
            if (input.closest('[data-role="schedule-editor"]')) return;
            const original = cfg[key];
            if (input.type === "checkbox") {
                result[key] = input.checked;
            } else if (input.tagName === "SELECT") {
                // Чтобы соответствовать логике RssV7, даже если в оригинале были options,
                // отправляем только выбранное значение, иначе сервер оборачивает его ещё раз в {value: ...}.
                result[key] = input.value;
            } else if (typeof original === "number") {
                const n = Number(input.value);
                result[key] = Number.isFinite(n) ? n : original;
            } else {
                result[key] = input.value;
            }
        });

        const normalizedOriginal = normalizeConfigValues(cfg);
        const normalizedPayload = prepareConfigPayload(result, cfg);
        const diff = {};

        Object.entries(normalizedPayload).forEach(([key, value]) => {
            const hasOriginal = Object.prototype.hasOwnProperty.call(normalizedOriginal, key);
            if (!hasOriginal || normalizedOriginal[key] !== value) {
                diff[key] = value;
            }
        });

        return diff;
    }

    function collectScheduleRules(rootEl, originalRules, options = {}) {
        if (!rootEl || !isAdminManage) return null;

        const stepIdx = Number(rootEl.dataset ? rootEl.dataset.stepIdx : NaN);
        const hasDraft = !options.ignoreDraft
            && Number.isFinite(stepIdx)
            && state.scheduleDrafts
            && Object.prototype.hasOwnProperty.call(state.scheduleDrafts, stepIdx);

        if (hasDraft) {
            return state.scheduleDrafts[stepIdx];
        }

        const editor = rootEl.querySelector('[data-role="schedule-editor"]');
        if (!editor) return Array.isArray(originalRules) ? originalRules : [];
        const rows = Array.from(editor.querySelectorAll('[data-role="schedule-row"]'));
        if (!rows.length) return [];

        return rows.map((row, idx) => {
            const raw = row._rawRule || (Array.isArray(originalRules) ? originalRules[idx] : {});
            const selectedDays = Array.isArray(row._selectedDays) ? row._selectedDays : null;
            const draft = {
                days: selectedDays ? stringifyDays(selectedDays) : ((row.querySelector('[data-schedule-field="days"]') || {}).value || ""),
                start: (row.querySelector('[data-schedule-field="start"]') || {}).value || "",
                end: (row.querySelector('[data-schedule-field="end"]') || {}).value || "",
                every: (row.querySelector('[data-schedule-field="every"]') || {}).value || "",
                label: (row.querySelector('[data-schedule-field="label"]') || {}).value || "",
            };
            return buildScheduleRulePayload(raw, draft);
        });
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

    async function applyDefaultsForCurrentAccount(btn) {
        if (!state.selectedAccountId || applyDefaultsInProgress) return;
        if (!state.selectedAccountHasDefaults) {
            alert("Для этого тарифа нет схемы по умолчанию.");
            return;
        }

        applyDefaultsInProgress = true;
        updateDefaultsButtonState();
        const originalText = btn ? btn.textContent : "";
        if (btn) {
            btn.disabled = true;
            btn.textContent = "Применяем...";
        }

        try {
            const url = replaceTemplate(state.applyDefaultsUrlTemplate, state.selectedAccountId);
            const resp = await fetch(url, { method: "POST" });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) {
                throw new Error((data && data.error) || "Не удалось применить настройки по умолчанию");
            }

            await loadSteps(state.selectedAccountId, {
                name: state.selectedAccountName,
                server: state.selectedServerName,
            });
            showMiniToast("Настройки сброшены на тарифные", "success");
        } catch (err) {
            console.error(err);
            alert(err && err.message ? err.message : "Не удалось применить настройки по умолчанию");
        } finally {
            applyDefaultsInProgress = false;
            if (btn) {
                btn.disabled = false;
                btn.textContent = originalText || "По умолчанию";
            }
            updateDefaultsButtonState();
        }
    }

    function setTemplateControlsVisibility(accountId) {
        if (!isAdminManage || !templateControls) return;
        templateControls.hidden = !accountId;
    }

    function setTemplateControlsLoading(isLoading) {
        if (!isAdminManage || !templateControls) return;
        state.templatesLoading = Boolean(isLoading);
        templateControls.classList.toggle("is-loading", state.templatesLoading);
        if (templateSelect) {
            templateSelect.disabled = state.templatesLoading
                || !templateSelect.options
                || !templateSelect.options.length;
        }
        if (templateApplyBtn) {
            const hasValue = templateSelect && templateSelect.value;
            const hasOptions = templateSelect && templateSelect.options.length > 1;
            templateApplyBtn.disabled = state.templatesLoading || !hasValue || !hasOptions;
        }
    }

    function renderTemplateOptions(templates) {
        if (!isAdminManage || !templateSelect) return;
        const items = Array.isArray(templates) ? templates : [];
        templateSelect.innerHTML = "";

        const placeholder = document.createElement("option");
        placeholder.value = "";
        placeholder.textContent = items.length ? "Выберите шаблон" : "Шаблоны не найдены";
        placeholder.selected = true;
        templateSelect.appendChild(placeholder);

        items.forEach((name) => {
            const option = document.createElement("option");
            option.value = name;
            option.textContent = name;
            templateSelect.appendChild(option);
        });

        if (templateApplyBtn) {
            const hasOptions = templateSelect.options.length > 1;
            templateApplyBtn.disabled = !hasOptions || state.templatesLoading;
        }
    }

    async function loadTemplatesForAccount(accountId) {
        if (!isAdminManage || !accountId || !templateControls) return;
        setTemplateControlsVisibility(accountId);
        renderTemplateOptions([]);

        if (state.templatesCache && state.templatesCache[accountId]) {
            renderTemplateOptions(state.templatesCache[accountId]);
            setTemplateControlsLoading(false);
            return;
        }

        setTemplateControlsLoading(true);
        try {
            const resp = await fetch(`/manage/account/${accountId}/templates`, {
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) {
                throw new Error(data.error || "Не удалось загрузить шаблоны");
            }

            const templates = Array.isArray(data.templates) ? data.templates : [];
            if (!state.templatesCache) state.templatesCache = {};
            state.templatesCache[accountId] = templates;
            renderTemplateOptions(templates);
        } catch (err) {
            console.error(err);
            showMiniToast(err.message || "Не удалось загрузить список шаблонов", "error");
            renderTemplateOptions([]);
        } finally {
            setTemplateControlsLoading(false);
        }
    }

    async function applyTemplateForCurrentAccount() {
        if (!isAdminManage || !state.selectedAccountId) return;
        const templateName = (templateSelect && templateSelect.value ? templateSelect.value : "").trim();
        if (!templateName) {
            alert("Выберите шаблон для применения.");
            return;
        }

        const originalText = templateApplyBtn ? templateApplyBtn.textContent : "";
        if (templateApplyBtn) {
            templateApplyBtn.disabled = true;
            templateApplyBtn.textContent = "Применяем...";
        }

        setTemplateControlsLoading(true);
        try {
            const resp = await fetch(`/manage/account/${state.selectedAccountId}/apply-template`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ template: templateName }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) {
                throw new Error(data.error || data.message || "Не удалось применить шаблон");
            }

            showMiniToast(data.message || "Шаблон применён", "success");
            await loadSteps(state.selectedAccountId, {
                name: state.selectedAccountName,
                server: state.selectedServerName,
            });
        } catch (err) {
            console.error(err);
            alert(err && err.message ? err.message : "Не удалось применить шаблон");
        } finally {
            setTemplateControlsLoading(false);
            if (templateApplyBtn) {
                templateApplyBtn.textContent = originalText || "Применить";
                const hasValue = templateSelect && templateSelect.value;
                const hasOptions = templateSelect && templateSelect.options.length > 1;
                templateApplyBtn.disabled = !hasValue || !hasOptions;
            }
        }
    }

    function scheduleConfigAutoSave(stepIdx, formEl, cfg) {
        if (!formEl || !cfg || formEl !== currentConfigForm) return;
        if (Number(formEl.dataset.stepIdx) !== stepIdx) return;

        if (configAutoSaveTimer) {
            clearTimeout(configAutoSaveTimer);
        }

        configAutoSaveTimer = null;
        saveConfig(stepIdx, formEl, cfg, { isAuto: true });
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
        const step = state.rawSteps && state.rawSteps[stepIdx];
        const payload = collectConfig(formEl, cfg || {});
        const scheduleRules = collectScheduleRules(formEl, step && step.ScheduleRules);
        const requestBody = { Config: payload };
        if (scheduleRules !== null) {
            requestBody.ScheduleRules = scheduleRules;
        }
        try {
            const url = replaceStepTemplate(state.updateUrlTemplate, state.selectedAccountId, stepIdx);
            const resp = await fetch(url, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(requestBody),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) throw new Error(data.error || "Ошибка сохранения");
            if (state.rawSteps[stepIdx]) {
                const currentCfg = state.rawSteps[stepIdx].Config || {};
                const mergedCfg = { ...currentCfg };

                Object.entries(payload).forEach(([key, value]) => {
                    const existing = currentCfg[key];
                    const hasValueField = existing && typeof existing === "object" && Object.prototype.hasOwnProperty.call(existing, "value");
                    if (hasValueField) {
                        mergedCfg[key] = { ...existing, value };
                    } else {
                        mergedCfg[key] = value;
                    }
                });

                state.rawSteps[stepIdx].Config = mergedCfg;
                if (scheduleRules !== null) {
                    state.rawSteps[stepIdx].ScheduleRules = scheduleRules || [];
                }
                if (state.scheduleDrafts && Object.prototype.hasOwnProperty.call(state.scheduleDrafts, stepIdx)) {
                    delete state.scheduleDrafts[stepIdx];
                }
                state.steps = buildViewStepsFromRaw(state.rawSteps, state.visibilityMap);
            }
            renderSteps();
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
        state.scheduleDrafts = {};
        state.selectedAccountId = accountId;
        state.selectedAccountHasDefaults = false;
        if (meta.name) state.selectedAccountName = meta.name;
        if (meta.server) state.selectedServerName = meta.server;
        highlightAccount(accountId);
        renderEmptyState("Загружаем настройки...");
        updateDefaultsButtonState();
        if (isAdminManage) {
            setTemplateControlsVisibility(accountId);
            setTemplateControlsLoading(true);
        }

        try {
            const url = replaceTemplate(state.detailsUrlTemplate, accountId);
            const resp = await fetch(url);
            let data = {};
            try { data = await resp.json(); } catch (_) { data = {}; }

            const isOk = resp.ok && (data.ok !== false);
            if (!isOk) {
                throw new Error((data && data.error) || "Не удалось загрузить настройки.");
            }

            const normalized = extractStepsAndMenu(data.raw_steps || data.rawSteps || data.Data || data.data || data);
            const rawSteps = normalizeManageSteps(normalized.steps);
            const visibilityMap = normalizeVisibilityMap(data.visibility_map || data.visibilityMap || {});
            const scriptLabels = data.script_labels || data.scriptLabels || {};
            applyScriptLabels(scriptLabels);
            const viewSteps = buildViewStepsFromRaw(rawSteps, visibilityMap);
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
            const tariffPriceValue = (typeof account.tariff_price === "number" || typeof account.tariff_price === "string")
                ? account.tariff_price
                : (typeof account.tariff_plan_price === "number" || typeof account.tariff_plan_price === "string")
                    ? account.tariff_plan_price
                    : null;
            state.selectedAccountTariffPrice = tariffPriceValue;
            state.selectedAccountTariffName = account.tariff_name || "";
            state.selectedAccountHasDefaults = !!account.has_default_settings;
            state.steps = viewSteps || [];
            state.rawSteps = rawSteps || [];
            state.visibilityMap = visibilityMap;
            state.scriptLabels = Object.assign({}, state.scriptLabels || {}, scriptLabels);
            state.menu = menu;
            state.debugInfo = debug || {
                http_status: resp.status,
                payload_keys: data ? Object.keys(data) : [],
                normalized_steps: Array.isArray(rawSteps) ? rawSteps.length : 0,
            };

            updateDefaultsButtonState();
            if (isAdminManage) {
                loadTemplatesForAccount(accountId).catch((err) => console.error(err));
            }

            const startOnMobile = isMobile();
            const firstStepIdx = (viewSteps && viewSteps.length) ? viewSteps[0].raw_index : null;
            state.selectedStepIndex = (firstStepIdx !== null && !startOnMobile) ? firstStepIdx : null;
            if (!state.rawSteps.length || !viewSteps.length) {
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
            if (isAdminManage) {
                setTemplateControlsLoading(false);
            }
        }
    }

    function handleAccountClick(event) {
        if (
            event.target.closest('[data-role="account-toggle"]')
            || event.target.closest('.manage-modern__account-toggle')
        ) {
            event.stopPropagation();
            return;
        }
        const btn = event.target.closest('[data-account-id]');
        if (!btn) return;
        if (event.preventDefault) {
            event.preventDefault();
        }
        const accountId = btn.dataset.accountId;
        if (!accountId) return;
        updatePaymentWarning(btn);
        loadSteps(accountId, {
            name: btn.dataset.accountName,
            server: btn.dataset.serverName,
        });
        if (isMobile()) {
            setMobileView('steps');
        }
    }

    function handleAccountToggle(event) {
        const toggleInput = event.target.closest('input[data-role="account-toggle"]');
        if (!toggleInput) return;
        event.stopPropagation();
        const accountId = toggleInput.dataset.accountId;
        if (!accountId) return;
        toggleAccountActive(accountId, toggleInput.checked, toggleInput);
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

        const scheduleBtn = event.target.closest('[data-role="schedule-edit"]');
        if (scheduleBtn) {
            event.stopPropagation();
            const stepIdx = Number(scheduleBtn.dataset.stepIdx);
            if (!Number.isNaN(stepIdx)) {
                if (state.selectedStepIndex !== stepIdx) {
                    state.selectedStepIndex = stepIdx;
                    renderSteps();
                    renderConfig();
                    if (isMobile()) {
                        setMobileView('config');
                    }
                }
                openScheduleEditor(stepIdx);
            }
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
            accountsRoot.addEventListener('change', handleAccountToggle);
        }
        if (stepsRoot) {
            stepsRoot.addEventListener('click', handleStepsClick);
            stepsRoot.addEventListener('change', handleStepToggleChange);
        }
        if (mobileBackBtn) {
            mobileBackBtn.addEventListener('click', mobileBack);
        }
        if (accountSearchInput) {
            accountSearchInput.addEventListener('input', (event) => {
                applyAccountFilter(event.target && event.target.value);
            });
            applyAccountFilter(accountSearchInput.value);
        }

        if (defaultsToolbarBtn) {
            defaultsToolbarBtn.addEventListener('click', openDefaultsConfirm);
        }
        defaultsModalClosers.forEach((el) => {
            el.addEventListener('click', closeDefaultsConfirm);
        });
        if (defaultsConfirmBtn) {
            defaultsConfirmBtn.addEventListener('click', async () => {
                const originalText = defaultsConfirmBtn.textContent;
                defaultsConfirmBtn.disabled = true;
                defaultsConfirmBtn.textContent = "Сбрасываем...";
                closeDefaultsConfirm();
                try {
                    await applyDefaultsForCurrentAccount(defaultsToolbarBtn || defaultsConfirmBtn);
                } finally {
                    defaultsConfirmBtn.disabled = false;
                    defaultsConfirmBtn.textContent = originalText;
                }
            });
        }

        if (templateSelect && templateApplyBtn) {
            templateSelect.addEventListener('change', () => {
                const hasOptions = templateSelect.options.length > 1;
                templateApplyBtn.disabled = state.templatesLoading
                    || !templateSelect.value
                    || !hasOptions;
            });
            templateApplyBtn.addEventListener('click', applyTemplateForCurrentAccount);
        }

        if (copyOpenBtn) {
            copyOpenBtn.addEventListener('click', openCopyModal);
        }
        if (copyTabs.length) {
            copyTabs.forEach((tab) => {
                tab.addEventListener('click', () => {
                    if (tab.classList.contains("is-active")) {
                        return;
                    }
                    setCopyMode(tab.dataset.copyMode);
                });
            });
        }
        copyCloseBtns.forEach((btn) => {
            btn.addEventListener('click', closeCopyModal);
        });
        if (copySourceSelect) {
            copySourceSelect.addEventListener('change', () => {
                updateCopySourceMeta();
                renderTargetOptions();
                updateTargetsAllState();
                updateCopyConfirmState();
            });
        }
        if (copyTargetsWrap) {
            copyTargetsWrap.addEventListener('change', () => {
                updateTargetsAllState();
                updateCopyConfirmState();
            });
        }
        if (copyTargetsAll) {
            copyTargetsAll.addEventListener('change', toggleTargetsAll);
        }
        if (copyConfirmInput) {
            copyConfirmInput.addEventListener('change', updateCopyConfirmState);
        }
        if (copyConfirmBtn) {
            copyConfirmBtn.addEventListener('click', applyCopySettings);
        }
        if (copyModal) {
            copyModal.addEventListener('click', (event) => {
                if (event.target === copyModal) {
                    closeCopyModal();
                }
            });
        }

        if (defaultsModal) {
            defaultsModal.addEventListener('click', (event) => {
                if (event.target === defaultsModal) {
                    closeDefaultsConfirm();
                }
            });
        }

        document.addEventListener('keydown', (event) => {
            if (event.key === 'Escape') {
                closeDefaultsConfirm();
                closeCopyModal();
            }
        });

        bindSwipeNavigation();
        window.addEventListener('resize', handleResize);

        syncAccountsUi();

        updateDefaultsButtonState();
        updateCopyOpenState();
        if (isAdminManage) {
            setTemplateControlsVisibility(state.selectedAccountId);
            if (state.selectedAccountId) {
                loadTemplatesForAccount(state.selectedAccountId).catch((err) => console.error(err));
            }
        }

        state.visibilityMap = normalizeVisibilityMap(state.visibilityMap || state.visibility_map || {});

        if (state.rawSteps === undefined && state.raw_steps) {
            state.rawSteps = state.raw_steps;
        }
        if (!state.rawSteps && state.steps && state.steps.length && Array.isArray(state.raw_steps)) {
            state.rawSteps = state.raw_steps;
        }

        if (Array.isArray(state.rawSteps) && state.rawSteps.length) {
            state.steps = buildViewStepsFromRaw(state.rawSteps || [], state.visibilityMap);
        }

        // Если ничего не подгружено сервером (или пришёл пустой массив),
        // подгружаем настройки через AJAX, чтобы правая часть не оставалась пустой.
        if (state.selectedAccountId && (!state.rawSteps || !state.rawSteps.length)) {
            loadSteps(state.selectedAccountId);
            return;
        }

        if (state.selectedAccountId) {
            highlightAccount(state.selectedAccountId);
            const firstStepIdx = (state.steps && state.steps.length) ? state.steps[0].raw_index : null;
            if (firstStepIdx !== null && state.selectedStepIndex === null && !isMobile()) {
                state.selectedStepIndex = firstStepIdx;
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
