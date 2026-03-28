/* ══════════════════════════════════════════════════════════════
   EnPro Filtration Mastermind — Frontend App
   ══════════════════════════════════════════════════════════════ */

(function () {
    'use strict';

    // Inject clickable card link + simulate mode styles
    (function() {
        var style = document.createElement('style');
        style.textContent = [
            '.card-link { color: var(--accent); cursor: pointer; text-decoration: none; border-bottom: 1px dashed var(--accent); transition: color 0.15s; }',
            '.card-link:hover { color: var(--navy); border-bottom-color: var(--navy); }',
            '.sim-bar { position: fixed; top: 0; left: 0; right: 0; z-index: 2000; background: linear-gradient(135deg, #003366, #0066CC); color: white; padding: 12px 20px; display: flex; align-items: center; gap: 16px; box-shadow: 0 4px 16px rgba(0,0,0,0.2); font-family: inherit; }',
            '.sim-bar-title { font-weight: 700; font-size: 14px; white-space: nowrap; }',
            '.sim-bar-step { font-size: 13px; opacity: 0.9; white-space: nowrap; }',
            '.sim-bar-progress { flex: 1; height: 6px; background: rgba(255,255,255,0.2); border-radius: 3px; overflow: hidden; }',
            '.sim-bar-fill { height: 100%; background: white; border-radius: 3px; transition: width 0.5s ease; }',
            '.sim-bar-btns { display: flex; gap: 8px; }',
            '.sim-bar-btn { background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.3); color: white; padding: 5px 14px; border-radius: 6px; font-size: 12px; cursor: pointer; font-family: inherit; font-weight: 500; transition: background 0.15s; }',
            '.sim-bar-btn:hover { background: rgba(255,255,255,0.3); }',
            '.sim-narration { position: fixed; top: 52px; left: 0; right: 0; z-index: 1999; background: rgba(0,51,102,0.95); color: white; padding: 10px 20px; font-size: 14px; font-style: italic; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }',
            '@media (max-width: 600px) { .sim-bar { padding: 8px 12px; gap: 8px; flex-wrap: wrap; } .sim-bar-title { font-size: 12px; } .sim-narration { font-size: 12px; top: 60px; } }',
            '.ac-item:hover { background: #f0f4ff; }',
        ].join('\n');
        document.head.appendChild(style);
    })();

    // ── Config ──
    const API_BASE = window.ENPRO_API_BASE || '';
    const SESSION_KEY = 'enpro_fm_session';
    const HISTORY_KEY = 'enpro_fm_history';

    // ── State ──
    let sessionId = localStorage.getItem(SESSION_KEY);
    if (!sessionId) {
        sessionId = crypto.randomUUID ? crypto.randomUUID() : uuidFallback();
        localStorage.setItem(SESSION_KEY, sessionId);
    }
    let lastFollowUps = [];   // Track numbered options
    let isLoading = false;
    let searchCount = 0;      // Track searches for auto-reset

    // ── Autocomplete suggestions ──
    var AUTOCOMPLETE_ITEMS = [
        // Product Types
        {text: 'search filter cartridge', label: 'Filter Cartridge', type: 'Product'},
        {text: 'search filter element', label: 'Filter Element', type: 'Product'},
        {text: 'search filter bag', label: 'Filter Bag', type: 'Product'},
        {text: 'search filter housing', label: 'Filter Housing', type: 'Product'},
        {text: 'search depth sheet', label: 'Depth Sheet', type: 'Product'},
        {text: 'search membrane', label: 'Membrane', type: 'Product'},
        {text: 'search o-ring seal', label: 'O-Ring / Seal', type: 'Product'},
        // Specs
        {text: 'search 1 micron filter', label: '1 Micron', type: 'Spec'},
        {text: 'search 5 micron filter', label: '5 Micron', type: 'Spec'},
        {text: 'search 10 micron filter', label: '10 Micron', type: 'Spec'},
        {text: 'search 25 micron filter', label: '25 Micron', type: 'Spec'},
        {text: 'search polypropylene', label: 'Polypropylene', type: 'Media'},
        {text: 'search PTFE', label: 'PTFE', type: 'Media'},
        {text: 'search glass fiber', label: 'Glass Fiber', type: 'Media'},
        {text: 'search stainless steel', label: 'Stainless Steel', type: 'Media'},
        // Chemicals
        {text: 'chemical compatibility of sulfuric acid', label: 'Sulfuric Acid', type: 'Chemical'},
        {text: 'chemical compatibility of acetone', label: 'Acetone', type: 'Chemical'},
        {text: 'chemical compatibility of sodium hydroxide', label: 'Sodium Hydroxide', type: 'Chemical'},
        {text: 'chemical compatibility of hydrochloric acid', label: 'Hydrochloric Acid', type: 'Chemical'},
        // Manufacturers
        {text: 'manufacturer Pall', label: 'Pall', type: 'Manufacturer'},
        {text: 'manufacturer Flowserve', label: 'Flowserve', type: 'Manufacturer'},
        {text: 'manufacturer Graver', label: 'Graver', type: 'Manufacturer'},
        {text: 'manufacturer Filtrox', label: 'Filtrox', type: 'Manufacturer'},
        {text: 'manufacturer PPC', label: 'PPC', type: 'Manufacturer'},
        {text: 'manufacturer Lechler', label: 'Lechler', type: 'Manufacturer'},
    ];

    // ── DOM refs ──
    const chatArea = document.getElementById('chatArea');
    const userInput = document.getElementById('userInput');
    const sendBtn = document.getElementById('sendBtn');
    const typingEl = document.getElementById('typingIndicator');
    const modalOverlay = document.getElementById('modalOverlay');
    const modalTitle = document.getElementById('modalTitle');
    const modalLabel = document.getElementById('modalLabel');
    const modalInput = document.getElementById('modalInput');
    const modalHint = document.getElementById('modalHint');

    let currentModalType = null;

    // ── UUID fallback ──
    function uuidFallback() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = Math.random() * 16 | 0;
            return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
        });
    }

    // ── Time formatting ──
    function timeStr() {
        return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }

    function normalizeVoiceQuery(text) {
        if (!text) return '';

        var normalized = String(text)
            .replace(/\bpal\b/gi, 'Pall')
            .replace(/\bpower\s+parts?\b/gi, 'PowerFlow')
            .replace(/\bfiltrox\b/gi, 'Filtrox')
            .replace(/\bgraver\b/gi, 'Graver')
            .replace(/\bppc\b/gi, 'PPC')
            .replace(/\bflow\s*serve\b/gi, 'Flowserve')
            .replace(/[“”]/g, '"')
            .replace(/[\u2018\u2019]/g, "'");

        normalized = normalized
            .replace(/\b(yeah|ok|okay|you know|like|um|uh|sort of|kind of|whatever)\b/gi, ' ')
            .replace(/\s+/g, ' ')
            .trim();

        return normalized;
    }

    function detectManufacturerHint(text) {
        if (!text) return '';
        var haystack = String(text).toLowerCase();
        var vendors = ['Pall', 'PowerFlow', 'Flowserve', 'Graver', 'Filtrox', 'PPC', 'Lechler'];
        for (var i = 0; i < vendors.length; i += 1) {
            if (haystack.indexOf(vendors[i].toLowerCase()) !== -1) {
                return vendors[i];
            }
        }
        return '';
    }

    function buildVoiceFallbackActions(transcript, data) {
        var cleaned = normalizeVoiceQuery((data && (data.cleaned_transcript || data.transcript)) || transcript || '');
        var actions = [];
        var lower = cleaned.toLowerCase();
        var manufacturer = detectManufacturerHint(cleaned);

        if (cleaned) {
            actions.push({
                label: 'Search broader',
                query: 'search ' + cleaned
            });
        }

        if (manufacturer) {
            actions.push({
                label: 'Show ' + manufacturer,
                query: 'manufacturer ' + manufacturer
            });
        }

        if (/\b(chemical|compatibility|compatible)\b/i.test(lower)) {
            actions.push({
                label: 'Chemical compatibility',
                query: 'check chemical compatibility for ' + cleaned
            });
        }

        if (/\b(price|pricing|cost|quote)\b/i.test(lower)) {
            actions.push({
                label: 'Pricing',
                query: 'what is the price of ' + cleaned
            });
        }

        if (/\b(stock|inventory|available|availability|lead time|ship|shipping)\b/i.test(lower)) {
            actions.push({
                label: 'Availability',
                query: 'is ' + cleaned + ' in stock'
            });
        }

        if (/\b(compare|similar|alternate|alternates|alternatives|other manufacturers|other options)\b/i.test(lower)) {
            actions.push({
                label: 'Find alternates',
                query: 'find alternates for ' + cleaned
            });
        }

        if (!actions.length) {
            actions.push({
                label: 'What are you trying to look for?',
                query: 'lookup'
            });
        }

        // Keep the UI focused: only show the most useful next steps.
        var deduped = [];
        var seen = {};
        actions.forEach(function (action) {
            var key = (action.query || '').toLowerCase();
            if (!key || seen[key]) return;
            seen[key] = true;
            deduped.push(action);
        });

        return deduped.slice(0, 4);
    }

    function renderVoiceFallbackCard(transcript, data) {
        var cleaned = normalizeVoiceQuery((data && data.cleaned_transcript) || transcript || '');
        var actions = buildVoiceFallbackActions(transcript, data);
        var html = '<div class="chemical-card">';
        html += '<div class="chemical-card-header">I did not find an exact match</div>';
        html += '<div class="chemical-card-body">';
        html += '<div style="margin-bottom:10px; color:var(--text); font-size:14px; line-height:1.5;">';
        html += 'Let us narrow it down from the last query.';
        if (cleaned) {
            html += '<div style="margin-top:6px; color:var(--text-light); font-size:13px;">Cleaned query: ' + esc(cleaned) + '</div>';
        }
        html += '</div>';

        html += '<div style="display:flex; flex-wrap:wrap; gap:8px;">';
        actions.forEach(function (action) {
            html += '<button class="followup-btn" onclick="sendMessage(\'' + esc(action.query).replace(/'/g, "\\'") + '\')">' + esc(action.label) + '</button>';
        });
        html += '</div>';

        if (data && data.suggestions && data.suggestions.length > 0) {
            html += '<div style="margin-top:12px; padding-top:12px; border-top:1px solid var(--border);">';
            html += '<div style="font-size:11px; text-transform:uppercase; color:var(--text-light); font-weight:700; letter-spacing:0.5px; margin-bottom:6px;">Normalization hints</div>';
            data.suggestions.forEach(function (s) {
                html += '<div style="font-size:13px; margin-bottom:4px; color:var(--text);">';
                html += esc(String(s.field || 'field')) + ': "' + esc(String(s.original || '')) + '" → <strong>' + esc(String(s.resolved || '')) + '</strong>';
                html += '</div>';
            });
            html += '</div>';
        }

        html += '</div></div>';
        return html;
    }

    // ── Auto-grow textarea ──
    window.autoGrow = function (el) {
        el.style.height = 'auto';
        el.style.height = Math.min(el.scrollHeight, 100) + 'px';
    };

    // ── Keyboard handling ──
    window.handleKeyDown = function (e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    // ── Autocomplete ──
    var acDropdown = null;

    function showAutocomplete(query) {
        if (!query || query.length < 2) { hideAutocomplete(); return; }
        var q = query.toLowerCase();
        var matches = AUTOCOMPLETE_ITEMS.filter(function(item) {
            return item.label.toLowerCase().includes(q) || item.text.toLowerCase().includes(q);
        }).slice(0, 6);

        if (matches.length === 0) { hideAutocomplete(); return; }

        if (!acDropdown) {
            acDropdown = document.createElement('div');
            acDropdown.id = 'acDropdown';
            acDropdown.style.cssText = 'position:absolute; bottom:100%; left:0; right:0; background:white; border:1px solid var(--border); border-radius:8px 8px 0 0; box-shadow:0 -4px 12px rgba(0,0,0,0.1); max-height:240px; overflow-y:auto; z-index:500; display:none;';
            userInput.parentElement.style.position = 'relative';
            userInput.parentElement.appendChild(acDropdown);
        }

        var html = '';
        matches.forEach(function(item, idx) {
            html += '<div class="ac-item" data-idx="' + idx + '" style="padding:10px 14px; cursor:pointer; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #f0f0f0; font-size:13px;" onmousedown="selectAutocomplete(' + idx + ')">';
            html += '<span style="font-weight:500;">' + item.label + '</span>';
            html += '<span style="font-size:11px; color:var(--text-light); background:var(--bg); padding:2px 8px; border-radius:10px;">' + item.type + '</span>';
            html += '</div>';
        });
        acDropdown.innerHTML = html;
        acDropdown.style.display = 'block';
        acDropdown._matches = matches;
    }

    function hideAutocomplete() {
        if (acDropdown) acDropdown.style.display = 'none';
    }

    window.selectAutocomplete = function(idx) {
        if (!acDropdown || !acDropdown._matches) return;
        var item = acDropdown._matches[idx];
        if (item) {
            userInput.value = '';
            hideAutocomplete();
            sendMessage(item.text);
        }
    };

    // Wire autocomplete to input
    userInput.addEventListener('input', function() {
        showAutocomplete(userInput.value.trim());
    });
    userInput.addEventListener('blur', function() {
        setTimeout(hideAutocomplete, 200);
    });

    // ── Send handler ──
    window.handleSend = function () {
        const text = userInput.value.trim();
        if (!text || isLoading) return;

        // Check for numbered option shortcut
        const num = parseInt(text);
        if (num >= 1 && num <= lastFollowUps.length && text === String(num)) {
            const resolved = lastFollowUps[num - 1];
            userInput.value = '';
            userInput.style.height = 'auto';
            sendMessage(resolved);
            return;
        }

        userInput.value = '';
        userInput.style.height = 'auto';
        sendMessage(text);
    };

    // ── Core: sendMessage ──
    window.sendMessage = async function (text) {
        if (isLoading) return;

        // Number shortcut: typing "1"-"4" clicks the corresponding action card
        var trimmed = text.trim();
        if (/^[1-4]$/.test(trimmed)) {
            var actionCard = document.querySelector('.action-card[data-action-num="' + trimmed + '"]:not(.action-done)');
            if (actionCard) {
                actionCard.click();
                return;
            }
        }

        clearWelcome();
        appendMessage('user', text);
        parseAndUpdateContext(text);
        setLoading(true);
        var queryStart = Date.now();

        try {
            const res = await fetch(API_BASE + '/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: text, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
            trackQuery(queryStart, data);
            trackSearch(text, data.intent);
        } catch (err) {
            appendMessage('bot', 'Connection error. Please check your network and try again.');
            console.error('Chat error:', err);
            trackError();
        } finally {
            setLoading(false);
        }
    };

    // ── Direct lookup ──
    window.doLookup = async function (partNumber, mode) {
        if (isLoading) return;
        mode = mode || 'exact';

        var modeLabel = mode === 'starts_with' ? 'Starts with' : mode === 'contains' ? 'Contains' : 'Lookup';
        clearWelcome();
        appendMessage('user', modeLabel + ': ' + partNumber);
        setLoading(true);

        try {
            if (mode === 'exact') {
                // Direct exact lookup
                var res = await fetch(API_BASE + '/api/lookup', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ part_number: partNumber, session_id: sessionId })
                });
                var data = await res.json();
                handleResponse(data);
            } else {
                // Starts-with or contains — use suggest to get matches, then show as search results
                var stockVal = document.getElementById('stockFilter') ? document.getElementById('stockFilter').value : 'all';
                var res = await fetch(API_BASE + '/api/suggest?q=' + encodeURIComponent(partNumber) + '&mode=' + mode + '&in_stock=' + stockVal);
                var data = await res.json();
                var suggestions = data.suggestions || [];
                if (suggestions.length === 0) {
                    appendCard(renderVoiceFallbackCard(partNumber, { transcript: partNumber, query: partNumber }));
                } else {
                    appendMessage('bot', formatMarkdown('Found **' + suggestions.length + '** matches ' + (mode === 'starts_with' ? 'starting with' : 'containing') + ' "' + partNumber + '" [V25 FILTERS]:'));
                    // Stagger card loading so results cascade in smoothly
                    await loadProductsStaggered(suggestions);
                }
            }
        } catch (err) {
            appendMessage('bot', 'Lookup failed. Please try again.');
            console.error('Lookup error:', err);
        } finally {
            setLoading(false);
        }
    };

    // Show suggestions as a consolidated list card — click any row to expand
    async function loadProductsStaggered(suggestions) {
        if (suggestions.length === 1) {
            // Single result — fetch full product card
            try {
                var res = await fetch(API_BASE + '/api/lookup', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ part_number: suggestions[0].Part_Number, session_id: sessionId })
                });
                var data = await res.json();
                if (data.found && data.product) {
                    appendCard(renderProductCard(data.product), false);
                    appendFollowUps(data.product.Part_Number || '');
                }
            } catch (err) {
                console.error('Product fetch error:', err);
            }
        } else {
            // Multiple — show consolidated card, click to expand
            appendCard(renderConsolidatedCard(
                suggestions.map(function (s) {
                    return {
                        Part_Number: s.Part_Number,
                        Description: s.Description || '',
                        Final_Manufacturer: s.Manufacturer || ''
                    };
                })
            ), true);
        }
    }

    // ── Search ──
    window.doSearch = async function (query) {
        if (isLoading) return;

        clearWelcome();
        appendMessage('user', 'Search: ' + query);
        setLoading(true);

        try {
            const res = await fetch(API_BASE + '/api/search', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: query, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
        } catch (err) {
            appendMessage('bot', 'Search failed. Please try again.');
            console.error('Search error:', err);
        } finally {
            setLoading(false);
        }
    };

    // ── Chemical check ──
    window.doChemical = async function (chemical) {
        if (isLoading) return;

        clearWelcome();
        appendMessage('user', 'Chemical compatibility: ' + chemical);
        setLoading(true);

        try {
            const res = await fetch(API_BASE + '/api/chemical', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ chemical: chemical, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
        } catch (err) {
            appendMessage('bot', 'Chemical check failed. Please try again.');
            console.error('Chemical error:', err);
        } finally {
            setLoading(false);
        }
    };

    // ── Response handler ──
    async function handleResponse(data) {
        if (!data) {
            appendMessage('bot', 'No response received.');
            return;
        }

        if (data.quote_state) {
            syncQuoteState(data.quote_state);
        }

        // Track compare intent for context card
        if (data.intent === 'compare' || (data.table && data.table.title && data.table.title.toLowerCase().includes('compare'))) {
            sessionContext.compared = true;
            renderContextCard();
        }

        // Chemical intent — parse GPT text into structured card
        if (data.intent === 'chemical' && data.response && !data.chemical) {
            var parsed = parseChemicalResponse(data.response);
            if (parsed) {
                appendCard(renderChemicalCard(parsed));
                // Show any extra text below the card (recommendations, notes)
                if (parsed.extras) {
                    appendMessage('bot', formatMarkdown(parsed.extras));
                }
                if (data.products && data.products.length > 0) {
                    await renderProductsBatched(data.products);
                }
                scrollToBottom();
                searchCount++;
                checkAutoReset();
                return;
            }
        }

        // Application intent — render as plain guidance and any returned products
        if (data.intent === 'application' && data.response) {
            appendMessage('bot', formatMarkdown(data.response));
            if (data.products && data.products.length > 0) {
                await renderProductsBatched(data.products);
            }
            scrollToBottom();
            searchCount++;
            checkAutoReset();
            return;
        }

        // Only capture specs into context from SINGLE product lookups, not multi-result searches
        if (data.products && data.products.length === 1) {
            updateContextFromProducts(data.products);
        }

        // Handle different response shapes
        if (data.products && Array.isArray(data.products) && data.products.length > 0) {
            // Only show cards — no redundant text dump above them
            await renderProductsBatched(data.products);
        } else if (data.results && Array.isArray(data.results) && data.results.length > 0) {
            if (data.total_found !== undefined) {
                var headerMsg = 'Found **' + data.total_found + '** products';
                if (data.total_found > data.results.length) headerMsg += ' (showing top ' + data.results.length + ')';
                headerMsg += ' [V25 FILTERS]:';
                appendMessage('bot', formatMarkdown(headerMsg));
            }
            await renderProductsBatched(data.results);
        } else if (data.chemical) {
            appendCard(renderChemicalCard(data.chemical));
        } else if (data.table) {
            appendCard(renderTableCard(data.table));
        } else if (data.product) {
            appendCard(renderProductCard(data.product));
            appendFollowUps(data.product.part_number || data.product.Part_Number || '');
        } else if (data.options && Array.isArray(data.options)) {
            if (data.text) appendMessage('bot', formatMarkdown(data.text));
            appendNumberedOptions(data.options);
        } else if (data.text || data.message || data.response) {
            const txt = data.text || data.message || data.response;
            appendMessage('bot', formatMarkdown(txt));

            // Check for embedded options
            if (data.follow_ups) {
                appendFollowUps('', data.follow_ups);
            }
        } else if (data.total_found !== undefined && data.query !== undefined) {
            // Search results with 0 matches
            appendCard(renderVoiceFallbackCard(data.query || '', data));
        } else if (typeof data === 'string') {
            appendMessage('bot', formatMarkdown(data));
        } else {
            appendMessage('bot', formatMarkdown(JSON.stringify(data, null, 2)));
        }

        scrollToBottom();
        searchCount++;
        checkAutoReset();
    }

    // ── New Chat / Auto-reset after 10 searches ──
    function checkAutoReset() {
        if (searchCount >= 10) {
            appendMessage('bot', formatMarkdown(
                '**Session limit reached (10 searches).** Starting a fresh chat to keep things fast.\n\n' +
                'Your previous results are cleared. Use the command bar above to continue.'
            ));
            setTimeout(function () { newChat(); }, 2000);
        }
    }

    window.newChat = function () {
        // Clear chat area
        chatArea.innerHTML = '';
        // Restore welcome
        chatArea.innerHTML = '<div class="welcome">' +
            '<div class="welcome-icon" style="font-size:48px;">&#128270;</div>' +
            '<h2>Filtration Mastermind</h2>' +
            '<p style="font-size:16px; line-height:1.6;">Just ask. Look up a part, compare products, check chemical compatibility, or ask for pricing — type it like you\'d say it.</p>' +
            '<p style="margin-top:8px; font-size:13px; color:var(--text-light);">19,600+ validated products. Real-time inventory. Real prices.</p>' +
            '</div>';
        // Reset state
        searchCount = 0;
        lastFollowUps = [];
        // Clear context lane
        if (typeof window.clearContext === 'function') window.clearContext();
        // New session ID
        sessionId = crypto.randomUUID ? crypto.randomUUID() : uuidFallback();
        localStorage.setItem(SESSION_KEY, sessionId);
        scrollToBottom();
    };

    // ── Render products: 1 card if single, consolidated card if multiple ──
    async function renderProductsBatched(products) {
        if (products.length === 1) {
            // Single result — full product card
            appendCard(renderProductCard(products[0]), false);
            appendFollowUps(products[0].Part_Number || products[0].part_number || '');
        } else if (products.length > 1) {
            // Multiple results — consolidated card with expandable rows
            appendCard(renderConsolidatedCard(products), true);
        }
    }

    // Consolidated card — one card, multiple products as rows
    window.renderConsolidatedCard = function (products) {
        var SHOW_LIMIT = 5;
        var cardId = 'consol_' + Date.now();
        var html = '<div class="product-card">';
        html += '<div class="product-card-header">' + products.length + ' Results Found</div>';
        html += '<div class="product-card-body" style="padding:0;">';

        products.forEach(function (p, idx) {
            var pn = p.Part_Number || p.part_number || '?';
            var desc = p.Description || p.description || '';
            var mfr = p.Final_Manufacturer || p.Manufacturer_Display || p.Manufacturer || p.manufacturer || '';
            var price = p.Price || p.price || '';
            var stock = p.Total_Stock || p.total_stock || 0;
            var priceDisplay = (price && price !== 'Contact EnPro for pricing') ? price : '';
            var stockDisplay = stock > 0 ? '<span style="color:var(--stock-green);font-weight:700;">' + stock + '</span>' : '<span style="color:var(--stock-red);">0</span>';
            var hiddenStyle = idx >= SHOW_LIMIT ? ' style="display:none;" data-extra="' + cardId + '"' : '';

            html += '<div class="consol-row" onclick="expandConsolRow(\'' + esc(pn) + '\', this)"' + hiddenStyle + '>';
            html += '<div class="consol-row-main">';
            html += '<div class="consol-pn"><a class="card-link" onclick="event.stopPropagation(); sendMessage(\'lookup ' + esc(pn).replace(/'/g, "\\'") + '\')">' + esc(pn) + '</a></div>';
            html += '<div class="consol-desc">' + esc(desc) + '</div>';
            html += '</div>';
            html += '<div class="consol-row-meta">';
            if (mfr) html += '<span class="consol-mfr">' + esc(mfr) + '</span>';
            if (priceDisplay) html += '<span class="consol-price">' + priceDisplay + '</span>';
            html += stockDisplay;
            html += '</div>';
            html += '</div>';
        });

        if (products.length > SHOW_LIMIT) {
            html += '<div class="show-more-btn" id="showMore_' + cardId + '" onclick="showMoreResults(\'' + cardId + '\', this)" style="text-align:center; padding:10px; cursor:pointer; color:var(--accent); font-weight:600; font-size:13px; border-top:1px solid var(--border);">';
            html += 'Show ' + (products.length - SHOW_LIMIT) + ' more results';
            html += '</div>';
        }

        html += '</div></div>';
        return html;
    };

    window.showMoreResults = function (cardId, btn) {
        var extras = document.querySelectorAll('[data-extra="' + cardId + '"]');
        extras.forEach(function (el) { el.style.display = ''; });
        if (btn) btn.remove();
    };

    // Expand a consolidated row into a full card
    window.expandConsolRow = function (partNumber, rowEl) {
        rowEl.style.pointerEvents = 'none';
        rowEl.style.opacity = '0.5';
        sendMessage('lookup ' + partNumber);
    };

    // ── Render product card ──
    window.renderProductCard = function (p) {
        // Handle both camelCase and PascalCase/snake_case column names
        var pn = p.Part_Number || p.part_number || 'Product';
        var desc = p.Description || p.description || '';
        var ext = p.Extended_Description || p.extended_description || '';
        var ptype = p.Product_Type || p.product_type || '';
        var mfg = p.Final_Manufacturer || p.Manufacturer || p.manufacturer || '';
        var micron = p.Micron || p.micron || '';
        var media = p.Media || p.media || '';
        var tempF = p.Max_Temp_F || p.temp_rating || '';
        var psi = p.Max_PSI || p.psi_rating || '';
        var flow = p.Flow_Rate || p.flow_rate || '';
        var eff = p.Efficiency || p.efficiency || '';
        var price = p.Price || p.price || p.Last_Sell_Price || '';
        var stock = p.Stock || p.stock || {};
        var totalStock = p.Total_Stock || p.total_stock || 0;

        var html = '<div class="product-card">';
        html += '<div class="product-card-header">Part Number: ' + esc(String(pn)) + '</div>';
        html += '<div class="product-card-body">';

        // Extended Description is primary (John's preference from Feb 25 meeting)
        // Regular Description is fallback
        var primaryDesc = ext || desc;
        var fields = [
            ['Description', primaryDesc],
            ['Product Type', ptype],
            ['Manufacturer', mfg]
        ];

        // Specs line (micron is clickable)
        var specs = [];
        if (micron && micron !== '0' && micron !== '0.0') specs.push('<a class="card-link" onclick="sendMessage(\'search ' + esc(String(micron)) + ' micron filters\')">' + esc(String(micron)) + ' Micron</a>');
        if (media) specs.push(esc(String(media)));
        if (tempF && tempF !== '0' && tempF !== '0.0') specs.push(esc(String(tempF)) + '°F');
        if (psi && psi !== '0' && psi !== '0.0') specs.push(esc(String(psi)) + ' PSI');
        if (flow) specs.push(esc(String(flow)));
        if (specs.length) {
            fields.push(['Specs', specs.map(function (spec) {
                return '<div class="spec-line">' + spec + '</div>';
            }).join('')]);
        }

        if (eff) fields.push(['Efficiency', eff]);

        fields.forEach(function (f) {
            if (f[1]) {
                html += '<div class="product-field">';
                html += '<div class="product-field-label">' + esc(f[0]) + '</div>';
                var val = String(f[1]);
                if (f[0] === 'Manufacturer') {
                    html += '<div class="product-field-value"><a class="card-link" onclick="sendMessage(\'manufacturer ' + esc(val).replace(/'/g, "\\'") + '\')">' + esc(val) + '</a></div>';
                } else if (f[0] === 'Product Type') {
                    html += '<div class="product-field-value"><a class="card-link" onclick="sendMessage(\'search ' + esc(val).replace(/'/g, "\\'") + '\')">' + esc(val) + '</a></div>';
                } else if (f[0] === 'Specs') {
                    // Specs already contain HTML links for micron
                    html += '<div class="product-field-value">' + val + '</div>';
                } else {
                    html += '<div class="product-field-value">' + esc(val) + '</div>';
                }
                html += '</div>';
            }
        });

        // Stock
        var inventoryEntries = [];
        if (stock && typeof stock === 'object' && Object.keys(stock).length > 0) {
            Object.keys(stock).forEach(function (loc) {
                if (loc === 'status') return;
                var qty = parseInt(stock[loc]) || 0;
                inventoryEntries.push({ location: loc, qty: qty });
            });
        }
        if (inventoryEntries.length > 0) {
            html += '<div class="stock-section">';
            html += '<div class="stock-title">Inventory by Location</div>';
            inventoryEntries.forEach(function (entry) {
                var badge = entry.qty >= 10 ? 'green' : entry.qty >= 3 ? 'orange' : 'red';
                html += '<div class="stock-row">';
                html += '<span>' + esc(entry.location) + '</span>';
                html += '<span class="stock-qty ' + badge + '">' + entry.qty + ' units</span>';
                html += '</div>';
            });
            html += '</div>';
        } else if (totalStock > 0) {
            html += '<div class="stock-section">';
            html += '<div class="stock-title">Inventory</div>';
            html += '<div class="stock-row"><span class="stock-qty green">' + totalStock + ' total units</span></div>';
            html += '</div>';
        } else {
            html += '<div class="stock-section">';
            html += '<div class="stock-title">Inventory</div>';
            html += '<div class="stock-row"><span class="stock-qty red">Out of stock</span></div>';
            html += '</div>';
        }

        // Price rows (primary + fallbacks)
        var priceEntries = [];
        if (price) priceEntries.push({ label: 'Displayed Price', value: price });
        if (p.Last_Sell_Price && p.Last_Sell_Price > 0) {
            priceEntries.push({ label: 'Last Sell Price', value: '$' + Number(p.Last_Sell_Price).toFixed(2) });
        }
        if (p.Price_1 && p.Price_1 > 0) {
            priceEntries.push({ label: 'Price 1', value: '$' + Number(p.Price_1).toFixed(2) });
        }
        if (priceEntries.length) {
            html += '<div class="price-section">';
            html += '<div class="stock-title">Pricing</div>';
            priceEntries.forEach(function (entry) {
                html += '<div class="price-row"><span>' + esc(entry.label) + '</span><span class="price-val">' + esc(entry.value) + '</span></div>';
            });
            html += '</div>';
        } else {
            html += '<div class="price-section">';
            html += '<div class="stock-title">Pricing</div>';
            html += '<div class="price-row"><span>Base</span><span class="price-val">Contact EnPro for pricing</span></div>';
            html += '</div>';
        }

        html += '</div>'; // body

        // Card actions removed � keeping it simple
        html += '</div>';


        html += '</div>';
        return html;
    };

    // ── Parse chemical GPT response into structured data ──
    function parseChemicalResponse(text) {
        if (!text) return null;

        // Extract chemical name
        var chemMatch = text.match(/(?:Chemical|Compatibility)[:\s]*\**([^*\n]+)\**/i);
        var chemical = chemMatch ? chemMatch[1].trim().replace(/^\*+|\*+$/g, '') : 'Chemical Compatibility';

        // Extract A/B/C/D ratings for materials
        var materials = ['Viton', 'EPDM', 'Buna-N', 'Buna N', 'Nylon', 'PTFE', 'PVDF', '316SS', '316 SS', 'Stainless'];
        var ratingPattern = /(?:^|\n)\s*\d*\.?\s*\**\s*(Viton|EPDM|Buna[- ]?N|Nylon|PTFE|PVDF|316\s*SS|Stainless)[^:]*:\s*\**\s*([ABCD])\b/gi;
        var compatibilities = [];
        var seen = {};
        var match;

        while ((match = ratingPattern.exec(text)) !== null) {
            var mat = match[1].trim();
            var grade = match[2].toUpperCase();
            var matKey = mat.toLowerCase().replace(/[\s-]/g, '');
            if (seen[matKey]) continue;
            seen[matKey] = true;

            var status, statusLabel;
            if (grade === 'A') { status = 'compatible'; statusLabel = 'A — Compatible'; }
            else if (grade === 'B') { status = 'limited'; statusLabel = 'B — Limited'; }
            else if (grade === 'C') { status = 'limited'; statusLabel = 'C — Caution'; }
            else { status = 'not compatible'; statusLabel = 'D — AVOID'; }

            compatibilities.push({ material: mat, status: statusLabel, grade: grade });
        }

        if (compatibilities.length === 0) return null;

        // Extract extras: recommended, avoid, considerations, recommendation
        var extras = '';
        var extraPatterns = [
            /(?:Recommended\s*Materials?)[:\s]*(.*)/i,
            /(?:Materials?\s*to\s*AVOID)[:\s]*(.*)/i,
            /(?:Key\s*Considerations?)[:\s]*(.*)/i,
            /(?:EnPro\s*Recommendation)[:\s]*(.*)/i
        ];
        var extraLines = [];
        extraPatterns.forEach(function (pat) {
            var m = text.match(pat);
            if (m && m[1].trim()) {
                extraLines.push(m[0].trim());
            }
        });
        if (extraLines.length) extras = extraLines.join('\n');

        // Clean up chemical name — remove "Chemical Compatibility —" prefix
        chemical = chemical.replace(/^Chemical\s*Compatibility\s*[-—]\s*/i, '').trim();
        if (!chemical || chemical.length < 2) chemical = 'Chemical Compatibility';

        return {
            chemical: chemical,
            compatibilities: compatibilities,
            notes: '',
            extras: extras
        };
    }

    // ── Parse application guidance GPT response into structured data ──
    // Supports V5 5-bullet format: Customer Focus, Lead Product, Talking Points, Key Question, Watch Out
    function parsePregameResponse(text) {
        if (!text || text.length < 50) return null;

        var industry = '';
        var concerns = [];
        var product = '';
        var question = '';
        var caseStudy = '';

        // Extract industry from first line or header
        var indMatch = text.match(/(?:Industry|Application|Sector|Pre-Call)[:\s]*\**([^*\n]+)/i);
        if (indMatch) industry = indMatch[1].trim();

        // V5 5-bullet format: look for labeled sections
        var focusMatch = text.match(/\*?\*?Customer Focus\*?\*?[:\s]*([^\n]+)/i);
        var leadMatch = text.match(/\*?\*?Lead Product\*?\*?[:\s]*([^\n]+(?:\n(?!\d+\.\s|\*\*)[^\n]+)*)/i);
        var talkMatch = text.match(/\*?\*?Talking Points?\*?\*?[:\s]*([^\n]+(?:\n(?!\d+\.\s|\*\*)[^\n]+)*)/i);
        var keyQMatch = text.match(/\*?\*?Key Question\*?\*?[:\s]*([^\n]+)/i);
        var watchMatch = text.match(/\*?\*?Watch Out\*?\*?[:\s]*([^\n]+(?:\n(?!\d+\.\s|\*\*)[^\n]+)*)/i);

        if (focusMatch || leadMatch || talkMatch) {
            // V5 format detected — use structured extraction
            if (focusMatch) concerns.push(focusMatch[1].trim());
            if (talkMatch) {
                // Split talking points by sub-numbers or dashes
                var tpText = talkMatch[1].trim();
                var tpItems = tpText.split(/(?:\d+\)|[-–—])\s*/);
                tpItems.forEach(function(tp) {
                    tp = tp.trim();
                    if (tp.length > 5) concerns.push(tp);
                });
                if (concerns.length < 2) concerns.push(tpText);
            }
            if (watchMatch) concerns.push('Watch out: ' + watchMatch[1].trim());
            if (leadMatch) product = leadMatch[1].replace(/\*\*/g, '').trim();
            if (keyQMatch) question = keyQMatch[1].replace(/[""\u201c\u201d']/g, '').trim();
        } else {
            // Legacy format: extract numbered items
            var numItems = text.match(/\d+\.\s+\*?\*?([^\n*]+)/g);
            if (numItems && numItems.length >= 2) {
                concerns = numItems.slice(0, 5).map(function(item) {
                    return item.replace(/^\d+\.\s*\*?\*?/, '').replace(/\*\*/g, '').trim();
                });
            }

            // Extract recommended product — be more specific to avoid false matches
            var prodMatch = text.match(/(?:Lead Product|#1 Product|Recommended Product|Primary Product)[:\s]*\**([^*\n]+)/i);
            if (prodMatch) product = prodMatch[1].trim();

            // Extract closing question
            var qMatch = text.match(/(?:Key Question|Closing Question|Ask)[:\s]*[""\u201c]?([^""\u201d\n]+)/i);
            if (qMatch) question = qMatch[1].trim().replace(/["']/g, '');
        }

        // Extract case study
        var caseMatch = text.match(/(?:case study|example|reference)[:\s]*\**([^*\n]+)/i);
        if (caseMatch) caseStudy = caseMatch[1].trim();

        // If we couldn't parse enough structure, return null — let it render as text
        if (concerns.length < 2 && !product && !question) return null;

        return {
            industry: industry,
            concerns: concerns,
            product: product,
            question: question,
            caseStudy: caseStudy,
            fullText: text
        };
    }

    // ── Render application guidance card ──
    window.renderPregameCard = function (data) {
        var html = '<div class="chemical-card">';
        html += '<div class="chemical-card-header">Application Guidance' + (data.industry ? ': ' + esc(data.industry) : '') + '</div>';
        html += '<div class="chemical-card-body">';

        if (data.concerns && data.concerns.length > 0) {
            html += '<div style="margin-bottom:12px;">';
            html += '<div style="font-size:11px; text-transform:uppercase; color:var(--text-light); font-weight:700; letter-spacing:0.5px; margin-bottom:6px;">Key Concerns</div>';
            data.concerns.forEach(function (c, i) {
                var searchTerm = c.replace(/[,()]/g, '').trim().substring(0, 40);
                html += '<div class="followup-btn" style="display:block; text-align:left; margin-bottom:4px; padding:6px 10px; font-size:13px; cursor:pointer;" onclick="sendMessage(\'search ' + esc(searchTerm).replace(/'/g, "\\'") + '\')">' + (i+1) + '. ' + esc(c) + '</div>';
            });
            html += '</div>';
        }

        if (data.product) {
            html += '<div style="margin-bottom:12px;">';
            html += '<div style="font-size:11px; text-transform:uppercase; color:var(--text-light); font-weight:700; letter-spacing:0.5px; margin-bottom:4px;">Recommended Product</div>';
            var prodSearch = data.product.replace(/[()]/g, '').trim();
            html += '<div class="followup-btn" style="display:inline-block; font-size:14px; font-weight:600; padding:6px 12px; cursor:pointer;" onclick="sendMessage(\'search ' + esc(prodSearch).replace(/'/g, "\\'") + '\')">' + esc(data.product) + '</div>';
            html += '</div>';
        }

        if (data.question) {
            html += '<div style="margin-bottom:12px; background:var(--bg); padding:10px 12px; border-radius:6px; border-left:3px solid var(--accent); cursor:pointer;" onclick="copyToClipboard(\'' + esc(data.question).replace(/'/g, "\\'") + '\', this)">';
            html += '<div style="font-size:11px; text-transform:uppercase; color:var(--text-light); font-weight:700; letter-spacing:0.5px; margin-bottom:4px;">Opening Question <span style="font-size:10px; font-weight:400;">(click to copy)</span></div>';
            html += '<div style="font-size:14px; font-style:italic; color:var(--text);">\u201c' + esc(data.question) + '\u201d</div>';
            html += '</div>';
        }

        if (data.caseStudy) {
            html += '<div style="margin-bottom:10px;">';
            html += '<div style="font-size:11px; text-transform:uppercase; color:var(--text-light); font-weight:700; letter-spacing:0.5px; margin-bottom:4px;">Case Study</div>';
            html += '<div style="font-size:13px; color:var(--text);">' + esc(data.caseStudy) + '</div>';
            html += '</div>';
        }

        // Chain action buttons — guide the rep to the next step
        html += '<div style="margin-top:14px; padding-top:12px; border-top:1px solid var(--border); display:flex; flex-wrap:wrap; gap:8px;">';

        if (data.product) {
            var searchQuery = data.product.replace(/[()]/g, '').trim();
            html += '<button class="followup-btn" onclick="sendMessage(\'search ' + esc(searchQuery).replace(/'/g, "\\'") + '\')">Find ' + esc(data.product.substring(0, 20)) + '</button>';
        }

        if (data.industry) {
            html += '<button class="followup-btn" onclick="sendMessage(\'chemical compatibility common in ' + esc(data.industry).replace(/'/g, "\\'") + '\')">Chemical Check</button>';
        }

        html += '<button class="followup-btn" onclick="sendMessage(\'show me more details\')">Full Prep</button>';
        html += '</div>';

        html += '</div></div>';
        return html;
    };

    // ── Render chemical card ──
    window.renderChemicalCard = function (data) {
        var html = '<div class="chemical-card">';
        html += '<div class="chemical-card-header">' + esc(data.chemical || 'Chemical Compatibility') + '</div>';
        html += '<div class="chemical-card-body">';

        if (data.compatibilities && Array.isArray(data.compatibilities)) {
            data.compatibilities.forEach(function (row) {
                var grade = (row.grade || '').toUpperCase();
                var status = (row.status || '').toLowerCase();

                // Determine styling from grade first, fall back to status text
                var rowCls, badgeCls, displayText;
                if (grade === 'A') {
                    rowCls = 'compatible'; badgeCls = 'green';
                    displayText = row.status || 'A — Compatible';
                } else if (grade === 'B') {
                    rowCls = 'limited'; badgeCls = 'orange';
                    displayText = row.status || 'B — Limited';
                } else if (grade === 'C') {
                    rowCls = 'limited'; badgeCls = 'orange';
                    displayText = row.status || 'C — Caution';
                } else if (grade === 'D') {
                    rowCls = 'not-compatible'; badgeCls = 'red';
                    displayText = row.status || 'D — AVOID';
                } else {
                    // Legacy fallback for status-only data
                    rowCls = status.includes('compatible') && !status.includes('not') ? 'compatible' :
                        status.includes('not') ? 'not-compatible' : 'limited';
                    badgeCls = rowCls === 'compatible' ? 'green' :
                        rowCls === 'not-compatible' ? 'red' : 'orange';
                    displayText = row.status || 'Unknown';
                }

                html += '<div class="compat-row ' + rowCls + '">';
                html += '<span><strong>' + esc(row.material || row.media || '') + '</strong></span>';
                html += '<span class="compat-badge ' + badgeCls + '">' + esc(displayText) + '</span>';
                html += '</div>';
            });
        }

        if (data.notes) {
            html += '<div style="margin-top: 10px; font-size: 12px; color: var(--text-light); font-style: italic;">' + esc(data.notes) + '</div>';
        }

        html += '</div></div>';
        return html;
    };

    // ── Render table card ──
    window.renderTableCard = function (data) {
        var html = '<div class="table-card">';
        if (data.title) {
            html += '<div class="table-card-header">' + esc(data.title) + '</div>';
        }
        html += '<div class="table-card-wrapper"><table>';

        if (data.headers && Array.isArray(data.headers)) {
            html += '<thead><tr>';
            data.headers.forEach(function (h) {
                html += '<th>' + esc(h) + '</th>';
            });
            html += '</tr></thead>';
        }

        if (data.rows && Array.isArray(data.rows)) {
            html += '<tbody>';
            data.rows.forEach(function (row) {
                html += '<tr>';
                if (Array.isArray(row)) {
                    row.forEach(function (cell) {
                        html += '<td>' + esc(String(cell)) + '</td>';
                    });
                } else if (typeof row === 'object') {
                    (data.headers || Object.keys(row)).forEach(function (k) {
                        html += '<td>' + esc(String(row[k] || '')) + '</td>';
                    });
                }
                html += '</tr>';
            });
            html += '</tbody>';
        }

        html += '</table></div></div>';
        return html;
    };

    // ── Contextual Action Panel (replaces simple follow-up buttons) ──
    function appendFollowUps(partNumber, customFollowUps) {
        if (customFollowUps) {
            // Custom follow-ups: render as simple buttons
            var container = document.createElement('div');
            container.className = 'followup-buttons';
            container.style.maxWidth = '85%';
            customFollowUps.forEach(function (fu, i) {
                var btn = document.createElement('button');
                btn.className = 'followup-btn';
                btn.textContent = fu;
                btn.onclick = function () { sendMessage(fu); };
                container.appendChild(btn);
            });
            chatArea.appendChild(container);
            scrollToBottom();
            return;
        }

        if (!partNumber) return;

        // Build contextual action panel
        var panelId = 'actionPanel_' + Date.now();
        var panel = document.createElement('div');
        panel.className = 'msg bot';
        panel.innerHTML = '<div class="action-panel" id="' + panelId + '">' +
            '<div class="action-panel-header">Next Steps for ' + esc(partNumber) + '</div>' +
            '<div class="action-grid">' +
                '<div class="action-card" onclick="runAction(\'chemical\', \'' + esc(partNumber) + '\', this)" data-action-num="1">' +
                    '<div class="action-num">1</div>' +
                    '<div class="action-label">Chemical Compatibility</div>' +
                    '<div class="action-desc">Material compatibility</div>' +
                '</div>' +
                '<div class="action-card" onclick="runAction(\'price\', \'' + esc(partNumber) + '\', this)" data-action-num="2">' +
                    '<div class="action-num">2</div>' +
                    '<div class="action-label">Price Check</div>' +
                    '<div class="action-desc">Quick pricing lookup</div>' +
                '</div>' +
                '<div class="action-card" onclick="runAction(\'availability\', \'' + esc(partNumber) + '\', this)" data-action-num="3">' +
                    '<div class="action-num">3</div>' +
                    '<div class="action-label">Availability</div>' +
                    '<div class="action-desc">Stock and inventory</div>' +
                '</div>' +
                '<div class="action-card" onclick="showCompareForm(\'' + esc(partNumber) + '\', \'' + panelId + '\')" data-action-num="4">' +
                    '<div class="action-num">4</div>' +
                    '<div class="action-label">Compare</div>' +
                    '<div class="action-desc">Side-by-side with another part</div>' +
                '</div>' +
            '</div>' +
        '</div>';

        chatArea.appendChild(panel);
        scrollToBottom();
    }

    // Run a simple action (no additional input needed)
    window.runAction = function (action, partNumber, el) {
        if (el) {
            el.classList.add('action-done');
            el.style.pointerEvents = 'none';
        }
        switch (action) {
            case 'chemical':
                sendMessage('chemical compatibility check for part ' + partNumber);
                break;
            case 'price':
                sendMessage('price ' + partNumber);
                break;
            case 'availability':
                sendMessage('lookup ' + partNumber);
                break;
        }
    };

    // Show compare — opens side panel with smart suggestions
    window.showCompareForm = function (partNumber, panelId) {
        // Instead of inline form, open compare side panel
        openComparePanel(partNumber);
    };

    // ── Compare Side Panel ──
    function openComparePanel(partNumber) {
        var panel = document.getElementById('comparePanel');
        var overlay = document.getElementById('comparePanelOverlay');
        var body = document.getElementById('comparePanelBody');

        document.getElementById('comparePanelTitle').textContent = 'Compare: ' + partNumber;
        body.innerHTML = '<div class="compare-loading">Finding similar products...</div>';

        panel.classList.add('open');
        overlay.classList.add('active');

        fetch(API_BASE + '/api/compare-suggestions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ part_number: partNumber })
        })
        .then(function (r) { return r.json(); })
        .then(function (data) {
            renderComparePanel(data, partNumber);
        })
        .catch(function (err) {
            body.innerHTML = '<div class="compare-empty">Could not load suggestions. Try again.</div>';
        });
    }

    window.closeComparePanel = function () {
        document.getElementById('comparePanel').classList.remove('open');
        document.getElementById('comparePanelOverlay').classList.remove('active');
    };

    function renderComparePanel(data, sourcePartNumber) {
        var body = document.getElementById('comparePanelBody');
        var html = '';

        // Source product card
        if (data.source) {
            var s = data.source;
            html += '<div class="compare-source">';
            html += '<div class="compare-source-label">Comparing</div>';
            html += '<div class="compare-source-pn">' + esc(s.Part_Number || sourcePartNumber) + '</div>';
            html += '<div class="compare-source-desc">' + esc(s.Description || '') + '</div>';
            html += '<div class="compare-source-specs">';
            if (s.Micron) html += '<span class="compare-spec-tag">Micron: ' + esc(String(s.Micron)) + '</span>';
            if (s.Media) html += '<span class="compare-spec-tag">' + esc(String(s.Media)) + '</span>';
            if (s.Max_Temp_F) html += '<span class="compare-spec-tag">' + esc(String(s.Max_Temp_F)) + '°F</span>';
            if (s.Max_PSI) html += '<span class="compare-spec-tag">' + esc(String(s.Max_PSI)) + ' PSI</span>';
            if (s.Final_Manufacturer) html += '<span class="compare-spec-tag">' + esc(String(s.Final_Manufacturer)) + '</span>';
            html += '</div></div>';
        }

        // Manual compare input — always visible
        html += '<div style="padding: 0 0 16px 0; border-bottom: 1px solid var(--border); margin-bottom: 16px;">';
        html += '<label style="font-size:13px; font-weight:600; margin-bottom:6px; display:block;">Compare with any part:</label>';
        html += '<div style="display:flex; gap:8px;">';
        html += '<input type="text" id="compareManualInput" placeholder="Enter part number..." onkeydown="if(event.key===\'Enter\')runCompareManual(\'' + esc(sourcePartNumber) + '\')" style="flex:1; padding:10px 12px; border:1px solid var(--border); border-radius:6px; font-size:14px;">';
        html += '<button class="quote-btn-primary" style="flex:none; padding:10px 16px;" onclick="runCompareManual(\'' + esc(sourcePartNumber) + '\')">Compare</button>';
        html += '</div></div>';

        // Categories
        if (data.categories && data.categories.length > 0) {
            html += '<div style="font-size:12px; text-transform:uppercase; color:var(--text-light); font-weight:700; letter-spacing:0.5px; margin-bottom:12px;">Or pick from suggestions:</div>';
            data.categories.forEach(function (cat) {
                html += '<div class="compare-category">';
                html += '<div class="compare-category-header">' + esc(cat.name) + '</div>';
                html += '<div class="compare-category-desc">' + esc(cat.desc || '') + '</div>';

                cat.products.forEach(function (p) {
                    var specParts = [];
                    if (p.Micron) specParts.push(p.Micron + ' micron');
                    if (p.Max_Temp_F) specParts.push(p.Max_Temp_F + '°F');
                    if (p.Max_PSI) specParts.push(p.Max_PSI + ' PSI');
                    var priceStr = p.Price || 'Contact EnPro';

                    html += '<div class="compare-suggestion" onclick="runCompareFromPanel(\'' + esc(sourcePartNumber) + '\', \'' + esc(p.Part_Number || '') + '\')">';
                    html += '<div class="compare-suggestion-info">';
                    html += '<div class="compare-suggestion-pn">' + esc(p.Part_Number || '') + '</div>';
                    html += '<div class="compare-suggestion-desc">' + esc(p.Description || '') + ' — ' + esc(String(priceStr)) + '</div>';
                    if (specParts.length) {
                        html += '<div class="compare-suggestion-specs">';
                        specParts.forEach(function (specPart) {
                            html += '<div class="spec-line">' + esc(specPart) + '</div>';
                        });
                        html += '</div>';
                    }
                    html += '</div>';
                    html += '<div class="compare-suggestion-action">Compare &rarr;</div>';
                    html += '</div>';
                });

                html += '</div>';
            });
        } else {
            html += '<div class="compare-empty">No similar products found in the catalog. Use the text box above to compare manually.</div>';
        }

        body.innerHTML = html;
    }

    window.runCompareFromPanel = function (sourcePn, targetPn) {
        closeComparePanel();
        sendMessage('compare ' + sourcePn + ' vs ' + targetPn);
    };

    window.runCompareManual = function (sourcePn) {
        var input = document.getElementById('compareManualInput');
        if (!input || !input.value.trim()) return;
        closeComparePanel();
        sendMessage('compare ' + sourcePn + ' vs ' + input.value.trim());
    };

    // ── Compare Selector (top-nav / quick action Compare button) ──
    window.openCompareSelector = function() {
        var panel = document.getElementById('comparePanel');
        var overlay = document.getElementById('comparePanelOverlay');
        var body = document.getElementById('comparePanelBody');

        document.getElementById('comparePanelTitle').textContent = 'Compare Products';

        var html = '';

        // Part A field — auto-fill if we have a pinned/last product
        var partA = '';
        if (sessionContext && sessionContext.pinnedPart) {
            partA = sessionContext.pinnedPart.Part_Number || '';
        }

        html += '<div style="margin-bottom:16px;">';
        html += '<label style="font-size:13px; font-weight:600; display:block; margin-bottom:6px;">Part A</label>';
        html += '<input type="text" id="comparePartA" value="' + (partA ? partA : '') + '" placeholder="Search or type part number..." style="width:100%; padding:10px 12px; border:1px solid var(--border); border-radius:6px; font-size:14px; box-sizing:border-box;">';
        html += '</div>';

        html += '<div style="margin-bottom:16px;">';
        html += '<label style="font-size:13px; font-weight:600; display:block; margin-bottom:6px;">Part B</label>';
        html += '<input type="text" id="comparePartB" placeholder="Search or type part number..." style="width:100%; padding:10px 12px; border:1px solid var(--border); border-radius:6px; font-size:14px; box-sizing:border-box;">';
        html += '</div>';

        html += '<button onclick="runCompareSelector()" style="width:100%; padding:12px; background:var(--accent); color:white; border:none; border-radius:8px; font-size:14px; font-weight:600; cursor:pointer; font-family:inherit;">Compare Side-by-Side</button>';

        html += '<div id="compareSelectorResults" style="margin-top:16px;"></div>';

        body.innerHTML = html;
        panel.classList.add('open');
        overlay.classList.add('active');

        // Focus the empty field
        var focusField = partA ? document.getElementById('comparePartB') : document.getElementById('comparePartA');
        setTimeout(function() { focusField.focus(); }, 200);

        // Add Enter key handlers
        var partAInput = document.getElementById('comparePartA');
        var partBInput = document.getElementById('comparePartB');
        partAInput.onkeydown = function(e) { if (e.key === 'Enter') partBInput.focus(); };
        partBInput.onkeydown = function(e) { if (e.key === 'Enter') runCompareSelector(); };
    };

    window.runCompareSelector = function() {
        var partA = (document.getElementById('comparePartA') || {}).value.trim();
        var partB = (document.getElementById('comparePartB') || {}).value.trim();

        if (!partA || !partB) {
            alert('Enter both part numbers to compare.');
            return;
        }

        closeComparePanel();
        if (typeof sessionContext !== 'undefined') sessionContext.compared = true;
        if (typeof renderContextCard === 'function') renderContextCard();
        sendMessage('compare ' + partA + ' vs ' + partB);
    };

    // Execute the compare
    window.runCompare = function (partNumber, panelId) {
        var input = document.getElementById('compareInput_' + panelId);
        if (!input || !input.value.trim()) return;
        var compareTo = input.value.trim();
        sendMessage('compare ' + partNumber + ' vs ' + compareTo);
    };

    // ── Quote Readiness Tracker ──
    var quoteStateData = null;

    function stateLineItems(state) {
        if (!state || !Array.isArray(state.line_items)) return [];
        return state.line_items
            .filter(function (item) { return item && item.resolved && item.resolved.part_number; })
            .map(function (item) {
                return {
                    part_number: item.resolved.part_number || '',
                    description: item.resolved.description || item.raw_input.description || '',
                    quantity: item.quantity || item.raw_input.quantity || 1,
                    price: item.resolved.price || '',
                    source: 'conversation'
                };
            });
    }

    function getCombinedQuoteItems() {
        var combined = [];
        var seen = {};

        stateLineItems(quoteStateData).forEach(function (item) {
            var key = (item.part_number || '').toUpperCase();
            if (!key) return;
            seen[key] = item;
            combined.push(item);
        });

        quoteItems.forEach(function (item) {
            var key = (item.part_number || '').toUpperCase();
            if (!key) return;
            if (seen[key]) {
                seen[key].quantity = item.quantity || seen[key].quantity || 1;
                if (item.price) seen[key].price = item.price;
                if (item.description) seen[key].description = item.description;
                seen[key].source = 'manual+conversation';
            } else {
                combined.push(item);
                seen[key] = item;
            }
        });

        return combined;
    }

    function buildQuoteNotesFromState() {
        if (!quoteStateData) return quoteData.notes || '';

        var notes = [];
        if (quoteData.notes) notes.push(quoteData.notes);

        if (quoteStateData.request) {
            if (quoteStateData.request.application) notes.push('Application: ' + quoteStateData.request.application);
            if (quoteStateData.request.chemical) notes.push('Chemical: ' + quoteStateData.request.chemical);
            if (quoteStateData.request.urgency) notes.push('Urgency: ' + quoteStateData.request.urgency);
        }

        if (Array.isArray(quoteStateData.open_questions) && quoteStateData.open_questions.length) {
            notes.push('Open questions: ' + quoteStateData.open_questions.join('; '));
        }

        return notes.filter(Boolean).join('\n');
    }

    function hydrateQuoteDataFromState() {
        var customer = quoteStateData && quoteStateData.customer ? quoteStateData.customer : {};
        quoteData.company = quoteData.company || customer.company_name || customer.account_name || '';
        quoteData.contact_name = quoteData.contact_name || customer.contact_name || '';
        quoteData.contact_email = quoteData.contact_email || customer.email || '';
        quoteData.contact_phone = quoteData.contact_phone || customer.phone || '';
        quoteData.ship_to = quoteData.ship_to || customer.ship_to || '';
        quoteData.items = getCombinedQuoteItems();
        quoteData.notes = buildQuoteNotesFromState();
    }

    async function fetchQuoteState() {
        try {
            var res = await fetch(API_BASE + '/api/quote-state/' + encodeURIComponent(sessionId));
            if (!res.ok) return null;
            var data = await res.json();
            if (data && data.quote_state) {
                syncQuoteState(data.quote_state);
                return data.quote_state;
            }
        } catch (err) {
            console.error('Quote state fetch error:', err);
        }
        return null;
    }

    function syncQuoteState(state) {
        quoteStateData = state || null;
        hydrateQuoteDataFromState();
        renderQuoteTracker();
        renderQuoteDrawer();
        if (document.getElementById('quoteModalOverlay').classList.contains('active')) {
            renderQuoteStep();
        }
    }

    function renderQuoteTracker() {
        var hasState = quoteStateData && (
            stateLineItems(quoteStateData).length ||
            (quoteStateData.customer && (quoteStateData.customer.company_name || quoteStateData.customer.account_name))
        );
        if (!hasState) {
            var existing = document.getElementById('quoteTracker');
            if (existing) existing.remove();
            return;
        }

        var tracker = document.getElementById('quoteTracker');
        if (!tracker) {
            tracker = document.createElement('div');
            tracker.id = 'quoteTracker';
            tracker.className = 'quote-tracker';
            // Insert before the input area
            var inputArea = document.querySelector('.input-area');
            inputArea.parentNode.insertBefore(tracker, inputArea);
        }

        var customerReady = !!(quoteStateData.customer && (quoteStateData.customer.company_name || quoteStateData.customer.account_name));
        var items = stateLineItems(quoteStateData);
        var lineReady = items.length > 0;
        var quantityReady = items.length > 0 && items.every(function (item) { return !!item.quantity; });
        var chemicalReady = !!(quoteStateData.request && quoteStateData.request.chemical);
        var doneCount = [customerReady, lineReady, quantityReady, chemicalReady].filter(Boolean).length;
        var primary = items[0] ? items[0].part_number : '';

        var html = '<div class="quote-tracker-inner">';
        html += '<div class="qt-title">Quote State</div>';
        html += '<div class="quote-tracker-part">' + esc(primary || 'Conversation quote in progress') + '</div>';
        html += '<div class="quote-tracker-steps">';
        [
            { label: 'Customer', done: customerReady },
            { label: 'Line Item', done: lineReady },
            { label: 'Quantity', done: quantityReady },
            { label: 'Chemical / Context', done: chemicalReady }
        ].forEach(function (step) {
            html += '<div class="qt-step ' + (step.done ? 'done' : '') + '">';
            html += '<span class="qt-check">' + (step.done ? '&#10003;' : '&#9675;') + '</span>';
            html += '<span class="qt-label">' + step.label + '</span>';
            html += '</div>';
        });
        html += '</div>';

        if (quoteStateData.ready_for_quote) {
            html += '<button class="qt-ready-btn" onclick="openQuoteModal()">Open Quote</button>';
        } else {
            html += '<div class="qt-progress">' + doneCount + '/4 fields captured</div>';
        }

        if (quoteStateData.open_questions && quoteStateData.open_questions.length) {
            html += '<div class="qt-progress" style="margin-top:8px;">Next: ' + esc(quoteStateData.open_questions[0]) + '</div>';
        }

        html += '</div>';
        tracker.innerHTML = html;
    }

    // Reset quote tracker on new chat
    var origNewChat = window.newChat;
    window.newChat = function () {
        var previousSessionId = sessionId;
        quoteStateData = null;
        quoteData = { step: 0, company: '', contact_name: '', contact_email: '', contact_phone: '', ship_to: '', items: [], notes: '' };
        quoteItems = [];
        localStorage.removeItem('enpro_quote_items');
        adminStats = { queries: 0, cost: 0, totalLatency: 0, errors: 0, reports: 0 };
        updateAdminFooter();
        var tracker = document.getElementById('quoteTracker');
        if (tracker) tracker.remove();
        origNewChat();
        fetch(API_BASE + '/api/quote-state/reset', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session_id: previousSessionId || sessionId })
        }).catch(function (err) {
            console.error('Quote state reset failed:', err);
        });
        renderQuoteDrawer();
    };

    // ── Numbered options ──
    function appendNumberedOptions(options) {
        lastFollowUps = options;

        var wrapper = document.createElement('div');
        wrapper.className = 'msg bot';

        var bubble = document.createElement('div');
        bubble.className = 'msg-bubble';

        var optDiv = document.createElement('div');
        optDiv.className = 'numbered-options';

        options.forEach(function (opt, i) {
            var row = document.createElement('div');
            row.className = 'numbered-option';
            row.onclick = function () { sendMessage(opt); };
            row.innerHTML = '<span class="option-num">' + (i + 1) + '</span><span>' + esc(opt) + '</span>';
            optDiv.appendChild(row);
        });

        bubble.appendChild(optDiv);
        wrapper.appendChild(bubble);
        chatArea.appendChild(wrapper);
        scrollToBottom();
    }

    // ── Append message ──
    function appendMessage(role, html) {
        var wrapper = document.createElement('div');
        wrapper.className = 'msg ' + role;

        var bubble = document.createElement('div');
        bubble.className = 'msg-bubble';
        bubble.innerHTML = html;

        var time = document.createElement('div');
        time.className = 'msg-time';
        time.textContent = timeStr();

        wrapper.appendChild(bubble);
        wrapper.appendChild(time);
        chatArea.appendChild(wrapper);
        scrollToBottom();
    }

    // ── Append raw card HTML ──
    function appendCard(cardHtml, staggered) {
        var wrapper = document.createElement('div');
        wrapper.className = 'msg bot';
        if (staggered) {
            wrapper.style.opacity = '0';
            wrapper.style.transform = 'translateY(12px)';
            wrapper.style.transition = 'opacity 0.3s ease-out, transform 0.3s ease-out';
        }
        wrapper.innerHTML = cardHtml;
        chatArea.appendChild(wrapper);
        if (staggered) {
            // Trigger animation on next frame
            requestAnimationFrame(function () {
                wrapper.style.opacity = '1';
                wrapper.style.transform = 'translateY(0)';
            });
        }
        scrollToBottom();
    }

    // ── Markdown-lite formatting ──
    function formatMarkdown(text) {
        if (!text) return '';
        // Strip internal data source labels
        text = text.replace(/\[V25 FILTERS\]/gi, '');
        text = text.replace(/\[V\d+ FILTERS?\]/gi, '');
        text = text.replace(/V25 FILTERS?/gi, '');
        text = text.replace(/\[NOT IN DATA\]/g, '');
        text = text.replace(/\[NO PRICE\]/g, '');
        text = text.replace(/\[CRITICAL DATA INTEGRITY RULE\][\s\S]*?\[USER MESSAGE\]:/g, '');
        // Strip follow-up option lines that GPT includes as text
        text = text.replace(/^.*(?:Application Guidance|quote ready|lookup|price|compare|manufacturer|chemical|application)[,\s]*(?:quote ready|help|lookup|price|compare|manufacturer|chemical|application)[,\s]*(?:help)?\.?\s*$/gim, '');
        // Clean up stray markdown artifacts
        text = text.replace(/^\s*[-•–]\s*/gm, '');  // Strip leading dashes/bullets
        text = text.replace(/^\s*#{1,3}\s+/gm, '');  // Strip heading markers
        text = text.replace(/\s{2,}/g, ' ');
        var s = esc(text);
        // Bold
        s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        // Strip remaining stray stars
        s = s.replace(/\*/g, '');
        // Inline code
        s = s.replace(/`(.+?)`/g, '<code>$1</code>');
        // Numbered lists — add proper spacing
        s = s.replace(/(\d+)\.\s+/g, '<br>$1. ');
        // Line breaks
        s = s.replace(/\n/g, '<br>');
        // Clean double breaks
        s = s.replace(/(<br\s*\/?>){3,}/g, '<br><br>');
        return s;
    }

    // ── HTML escape ──
    function esc(str) {
        if (!str) return '';
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // ── Loading state ──
    function setLoading(on) {
        isLoading = on;
        sendBtn.disabled = on;
        typingEl.classList.toggle('active', on);
        if (on) {
            chatArea.appendChild(typingEl);
            scrollToBottom();
        }
    }

    // ── Scroll ──
    function scrollToBottom() {
        requestAnimationFrame(function () {
            chatArea.scrollTop = chatArea.scrollHeight;
        });
    }

    // ── Clear welcome ──
    function clearWelcome() {
        var w = chatArea.querySelector('.welcome');
        if (w) w.remove();
    }

    // ── Modal management ──
    var lookupModeRow = document.getElementById('lookupModeRow');
    var lookupMode = document.getElementById('lookupMode');

    window.showModal = function (type) {
        currentModalType = type;
        modalOverlay.classList.add('active');

        // Show/hide lookup mode selector
        lookupModeRow.style.display = type === 'lookup' ? 'block' : 'none';

        // Show/hide dropdowns per modal type
        document.getElementById('chemicalSelect').style.display = type === 'chemical' ? 'block' : 'none';
        document.getElementById('manufacturerSelect').style.display = type === 'manufacturer' ? 'block' : 'none';
        document.getElementById('productTypeSelect').style.display = type === 'product_type' ? 'block' : 'none';
        document.getElementById('searchTags').style.display = type === 'search' ? 'block' : 'none';

        switch (type) {
            case 'lookup':
                modalTitle.textContent = 'Lookup Part';
                modalLabel.textContent = 'Part Number';
                modalInput.placeholder = 'e.g., EPF-1234';
                updateLookupHint();
                break;
            case 'chemical':
                modalTitle.textContent = 'Chemical Compatibility';
                modalLabel.textContent = 'Chemical Name';
                modalInput.placeholder = 'e.g., Sulfuric Acid';
                modalHint.textContent = 'Enter the chemical to check media compatibility.';
                break;
            case 'search':
                modalTitle.textContent = 'Search Products';
                modalLabel.textContent = 'Search Query';
                modalInput.placeholder = 'e.g., 10 micron bag filter';
                modalHint.textContent = 'Describe what you need — specs, type, application.';
                break;
            case 'manufacturer':
                modalTitle.textContent = 'Manufacturer Search';
                modalLabel.textContent = 'Manufacturer Name';
                modalInput.placeholder = 'e.g., Pall, Graver, Filtrox';
                modalHint.textContent = 'Enter manufacturer name to see their products.';
                break;
            case 'supplier':
                modalTitle.textContent = 'Supplier Code Lookup';
                modalLabel.textContent = 'Supplier Code';
                modalInput.placeholder = 'e.g., T1030000000';
                modalHint.textContent = 'Enter the supplier/OEM part number.';
                break;
            case 'product_type':
                modalTitle.textContent = 'Product Type Search';
                modalLabel.textContent = 'Product Type';
                modalInput.placeholder = 'e.g., Filter Cartridge';
                modalHint.textContent = 'Pick a product type from the list or type one.';
                break;
            case 'price':
                modalTitle.textContent = 'Price Check';
                modalLabel.textContent = 'Part Number';
                modalInput.placeholder = 'e.g., CLR130, EPE-10-5';
                modalHint.textContent = 'Enter the part number to check pricing.';
                break;
            case 'compare':
                modalTitle.textContent = 'Compare Products';
                modalLabel.textContent = 'Parts to Compare';
                modalInput.placeholder = 'e.g., CLR130 vs CLR140';
                modalHint.textContent = 'Enter part numbers separated by "vs" or spaces.';
                break;
        }

        modalInput.value = '';
        setTimeout(function () { modalInput.focus(); }, 100);
    };

    function updateLookupHint() {
        var mode = lookupMode.value;
        if (mode === 'exact') {
            modalHint.textContent = 'Enter the exact part number to look up.';
        } else if (mode === 'starts_with') {
            modalHint.textContent = 'Enter the beginning of a part number. Shows top 10 matches.';
        } else {
            modalHint.textContent = 'Enter any text contained in the part number. Shows top 10 matches.';
        }
    }

    lookupMode.addEventListener('change', function () {
        updateLookupHint();
        // Re-trigger suggestions if there's text
        if (modalInput.value.trim().length >= 2) {
            modalInput.dispatchEvent(new Event('input'));
        }
    });

    window.hideModal = function (e) {
        if (e && e.target !== modalOverlay) return;
        modalOverlay.classList.remove('active');
        currentModalType = null;
    };

    window.modalSubmit = function () {
        var val = modalInput.value.trim();
        if (!val) return;

        var type = currentModalType;
        var mode = lookupMode.value;
        hideModal();

        switch (type) {
            case 'lookup': doLookup(val, mode); break;
            case 'chemical': doChemical(val); break;
            case 'search': doSearch(val); break;
            case 'manufacturer': sendMessage('manufacturer ' + val); break;
            case 'supplier': sendMessage('supplier ' + val); break;
            case 'product_type': doSearch(val); break;
            case 'price': sendMessage('price ' + val); break;
            case 'compare': sendMessage('compare ' + val); break;
        }
    };

    // ── Typeahead / Autocomplete ──
    var suggestDropdown = document.getElementById('suggestDropdown');
    var suggestTimer = null;
    var suggestSelectedIndex = -1;
    var suggestItems = [];

    modalInput.addEventListener('input', function () {
        if (currentModalType !== 'lookup') return;
        var q = modalInput.value.trim();
        clearTimeout(suggestTimer);
        suggestSelectedIndex = -1;

        if (q.length < 2) {
            suggestDropdown.classList.remove('active');
            suggestDropdown.innerHTML = '';
            return;
        }

        // Debounce 250ms
        suggestTimer = setTimeout(function () {
            fetchSuggestions(q);
        }, 250);
    });

    // Arrow keys + Enter in the suggest dropdown
    modalInput.addEventListener('keydown', function (e) {
        if (currentModalType !== 'lookup') return;
        if (!suggestDropdown.classList.contains('active')) return;

        var items = suggestDropdown.querySelectorAll('.suggest-item');
        if (!items.length) return;

        if (e.key === 'ArrowDown') {
            e.preventDefault();
            suggestSelectedIndex = Math.min(suggestSelectedIndex + 1, items.length - 1);
            updateSuggestHighlight(items);
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            suggestSelectedIndex = Math.max(suggestSelectedIndex - 1, -1);
            updateSuggestHighlight(items);
        } else if (e.key === 'Enter' && suggestSelectedIndex >= 0) {
            e.preventDefault();
            e.stopPropagation();
            selectSuggestion(suggestItems[suggestSelectedIndex]);
        } else if (e.key === 'Escape') {
            suggestDropdown.classList.remove('active');
            suggestSelectedIndex = -1;
        }
    });

    function updateSuggestHighlight(items) {
        items.forEach(function (el, i) {
            el.classList.toggle('selected', i === suggestSelectedIndex);
        });
        if (suggestSelectedIndex >= 0 && items[suggestSelectedIndex]) {
            items[suggestSelectedIndex].scrollIntoView({ block: 'nearest' });
        }
    }

    async function fetchSuggestions(query) {
        suggestDropdown.innerHTML = '<div class="suggest-loading">Searching...</div>';
        suggestDropdown.classList.add('active');

        try {
            var mode = lookupMode.value || 'exact';
            var stockFlt = document.getElementById('stockFilter') ? document.getElementById('stockFilter').value : 'all';
            var res = await fetch(API_BASE + '/api/suggest?q=' + encodeURIComponent(query) + '&mode=' + mode + '&in_stock=' + stockFlt);
            var data = await res.json();
            suggestItems = data.suggestions || [];

            if (suggestItems.length === 0) {
                suggestDropdown.innerHTML = '<div class="suggest-loading">No matches found</div>';
                return;
            }

            var html = '';
            suggestItems.forEach(function (item, i) {
                html += '<div class="suggest-item" data-index="' + i + '">';
                html += '<div class="suggest-pn">' + esc(item.Part_Number || '') + '</div>';
                html += '<div class="suggest-desc">' + esc(item.Description || '') + '</div>';
                if (item.Manufacturer) {
                    html += '<div class="suggest-mfr">' + esc(item.Manufacturer) + '</div>';
                }
                html += '</div>';
            });
            suggestDropdown.innerHTML = html;

            // Click handlers
            suggestDropdown.querySelectorAll('.suggest-item').forEach(function (el) {
                el.addEventListener('click', function () {
                    var idx = parseInt(el.dataset.index);
                    selectSuggestion(suggestItems[idx]);
                });
            });
        } catch (err) {
            suggestDropdown.innerHTML = '<div class="suggest-loading">Search error</div>';
            console.error('Suggest error:', err);
        }
    }

    function selectSuggestion(item) {
        if (!item) return;
        suggestDropdown.classList.remove('active');
        suggestDropdown.innerHTML = '';
        suggestSelectedIndex = -1;
        // Always do exact lookup when clicking a specific suggestion
        var type = currentModalType;
        hideModal();
        doLookup(item.Part_Number, 'exact');
    }

    // Close dropdown when clicking outside
    document.addEventListener('click', function (e) {
        if (!e.target.closest('.modal-body')) {
            suggestDropdown.classList.remove('active');
            suggestSelectedIndex = -1;
        }
    });

    // Clear dropdown when modal closes
    var origHideModal = window.hideModal;
    window.hideModal = function (e) {
        suggestDropdown.classList.remove('active');
        suggestDropdown.innerHTML = '';
        suggestSelectedIndex = -1;
        suggestItems = [];
        origHideModal(e);
    };

    // ── Quick search from tags ──
    window.quickSearch = function (term) {
        hideModal();
        doSearch(term);
    };

    // ── Copy to clipboard (no email app) ──
    window.copyToClipboard = function (text, el) {
        navigator.clipboard.writeText(text).then(function () {
            // Show toast
            var toast = document.createElement('div');
            toast.className = 'copy-toast';
            toast.textContent = 'Copied: ' + text;
            document.body.appendChild(toast);
            setTimeout(function () { toast.remove(); }, 2000);
        }).catch(function () {
            // Fallback for older browsers
            var input = document.createElement('input');
            input.value = text;
            document.body.appendChild(input);
            input.select();
            document.execCommand('copy');
            document.body.removeChild(input);
            var toast = document.createElement('div');
            toast.className = 'copy-toast';
            toast.textContent = 'Copied: ' + text;
            document.body.appendChild(toast);
            setTimeout(function () { toast.remove(); }, 2000);
        });
    };

    // ── Print / Copy product card ──
    window.printCard = function (btn) {
        var card = btn.closest('.product-card');
        if (!card) return;
        var printWin = window.open('', '_blank', 'width=500,height=600');
        printWin.document.write('<html><head><title>EnPro Product</title>');
        printWin.document.write('<style>body{font-family:system-ui,sans-serif;padding:20px;color:#333;}');
        printWin.document.write('.product-card-header{background:#003366;color:white;padding:12px 16px;font-weight:700;font-size:16px;}');
        printWin.document.write('.product-card-body{padding:14px 16px;}.product-field{display:flex;padding:6px 0;border-bottom:1px solid #eee;font-size:13px;}');
        printWin.document.write('.product-field-label{font-weight:600;color:#666;min-width:130px;}.stock-section{margin-top:10px;padding-top:10px;border-top:2px solid #f5f5f5;}');
        printWin.document.write('.stock-title{font-weight:600;font-size:13px;margin-bottom:6px;}.stock-row{display:flex;justify-content:space-between;padding:4px 8px;font-size:13px;}');
        printWin.document.write('.product-price{margin-top:10px;padding:10px;background:#f5f5f5;border-radius:6px;font-weight:600;font-size:14px;}');
        printWin.document.write('.product-footer{padding:10px 16px;background:#f5f5f5;font-size:11px;text-align:center;border-top:1px solid #ddd;margin-top:10px;}');
        printWin.document.write('.card-actions{display:none;}.stock-qty{font-weight:700;}');
        printWin.document.write('</style></head><body>');
        printWin.document.write(card.outerHTML);
        printWin.document.write('<div style="margin-top:20px;text-align:center;font-size:11px;color:#999;">EnPro Inc. | service@enproinc.com | 1 (800) 323-2416</div>');
        printWin.document.write('</body></html>');
        printWin.document.close();
        printWin.print();
    };

    window.copyCard = function (btn) {
        var card = btn.closest('.product-card');
        if (!card) return;
        // Extract text content from card, formatted cleanly
        var header = card.querySelector('.product-card-header');
        var fields = card.querySelectorAll('.product-field');
        var price = card.querySelector('.product-price');
        var stockRows = card.querySelectorAll('.stock-row');

        var text = (header ? header.textContent.trim() : '') + '\n';
        text += '─'.repeat(40) + '\n';
        fields.forEach(function (f) {
            var label = f.querySelector('.product-field-label');
            var value = f.querySelector('.product-field-value');
            if (label && value) {
                text += label.textContent.trim() + ': ' + value.textContent.trim() + '\n';
            }
        });
        if (price) text += 'Price: ' + price.textContent.trim() + '\n';
        if (stockRows.length > 0) {
            text += 'Stock: ';
            stockRows.forEach(function (r) { text += r.textContent.trim().replace(/\s+/g, ' ') + ' | '; });
            text = text.slice(0, -3) + '\n';
        }
        text += '─'.repeat(40) + '\n';
        text += 'EnPro Inc. | service@enproinc.com | 1 (800) 323-2416\n';

        copyToClipboard(text, btn);
    };

    // ── Admin Stats Tracking ──
    var adminStats = {
        queries: 0,
        cost: 0,
        totalLatency: 0,
        errors: 0,
        reports: 0
    };

    function trackQuery(startTime, data) {
        adminStats.queries++;
        var latency = Date.now() - startTime;
        adminStats.totalLatency += latency;

        // Parse cost from response
        var costStr = (data && data.cost) ? data.cost : '$0';
        var costVal = parseFloat(costStr.replace(/[^0-9.]/g, '')) || 0;
        adminStats.cost += costVal;

        updateAdminFooter();
    }

    function trackError() {
        adminStats.errors++;
        updateAdminFooter();
    }

    function updateAdminFooter() {
        var el = function(id) { return document.getElementById(id); };
        el('statQueries').textContent = adminStats.queries;
        el('statCost').textContent = '$' + adminStats.cost.toFixed(2);
        var avg = adminStats.queries > 0 ? Math.round(adminStats.totalLatency / adminStats.queries) : 0;
        el('statLatency').textContent = avg + 'ms';
        el('statLatency').className = 'admin-value ' + (avg < 2000 ? 'good' : avg < 5000 ? '' : 'error');
        el('statErrors').textContent = adminStats.errors;
        el('statErrors').className = 'admin-value ' + (adminStats.errors > 0 ? 'error' : 'good');
        el('statReports').textContent = adminStats.reports;
        el('statReports').className = 'admin-value ' + (adminStats.reports > 0 ? 'error' : '');
    }

    // Report card to Peter
    window.reportCard = function (btn) {
        var card = btn.closest('.product-card');
        if (!card) return;

        var header = card.querySelector('.product-card-header');
        var partNumber = header ? header.textContent.replace('Part Number: ', '').trim() : 'Unknown';

        // Visual feedback
        btn.textContent = '\u2713 Reported';
        btn.style.pointerEvents = 'none';
        btn.style.color = 'var(--stock-green)';

        // Store the report
        adminStats.reports++;
        updateAdminFooter();

        // Build report data
        var reportData = {
            part_number: partNumber,
            timestamp: new Date().toISOString(),
            session_id: sessionId,
            card_html: card.outerHTML
        };

        // Save to localStorage for now (until backend endpoint exists)
        var reports = JSON.parse(localStorage.getItem('enpro_reports') || '[]');
        reports.push(reportData);
        localStorage.setItem('enpro_reports', JSON.stringify(reports));

        // Show toast
        var toast = document.createElement('div');
        toast.className = 'copy-toast';
        toast.textContent = 'Reported: ' + partNumber + ' \u2014 flagged for Peter';
        document.body.appendChild(toast);
        setTimeout(function () { toast.remove(); }, 2000);

        console.log('REPORT:', reportData);
    };

    // ── Dark Mode Toggle ──
    window.toggleDarkMode = function () {
        document.body.classList.toggle('dark-mode');
        var isDark = document.body.classList.contains('dark-mode');
        localStorage.setItem('enpro_dark_mode', isDark ? 'true' : 'false');
        document.getElementById('darkModeBtn').textContent = isDark ? '\u2600' : '\u263E';
    };

    // Restore dark mode preference
    if (localStorage.getItem('enpro_dark_mode') === 'true') {
        document.body.classList.add('dark-mode');
        document.getElementById('darkModeBtn').textContent = '\u2600';
    }

    // ── Expose for inline onclick ──
    window.sendMessage = sendMessage;

    // ── Load chemical names for dropdown ──
    (async function loadChemicals() {
        try {
            var res = await fetch(API_BASE + '/api/chemicals/list');
            var data = await res.json();
            var chemicals = data.chemicals || [];
            var select = document.getElementById('chemicalSelect');
            chemicals.forEach(function (chem) {
                var opt = document.createElement('option');
                opt.value = chem;
                opt.textContent = chem;
                select.appendChild(opt);
            });
            // When user picks from dropdown, fill the input and submit
            select.addEventListener('change', function () {
                if (select.value) {
                    modalInput.value = select.value;
                    modalSubmit();
                }
            });
        } catch (err) {
            console.error('Failed to load chemicals list:', err);
        }
    })();

    // ── Load manufacturer names for dropdown ──
    (async function loadManufacturers() {
        try {
            var res = await fetch(API_BASE + '/api/manufacturers/list');
            var data = await res.json();
            var manufacturers = data.manufacturers || [];
            var select = document.getElementById('manufacturerSelect');
            manufacturers.forEach(function (mfr) {
                var opt = document.createElement('option');
                opt.value = mfr;
                opt.textContent = mfr;
                select.appendChild(opt);
            });
            select.addEventListener('change', function () {
                if (select.value) {
                    modalInput.value = select.value;
                    modalSubmit();
                }
            });
        } catch (err) {
            console.error('Failed to load manufacturers list:', err);
        }
    })();

    // ── Load product types for dropdown ──
    (async function loadProductTypes() {
        try {
            var res = await fetch(API_BASE + '/api/product-types/list');
            var data = await res.json();
            var types = data.product_types || [];
            var select = document.getElementById('productTypeSelect');
            types.forEach(function (t) {
                var opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t;
                select.appendChild(opt);
            });
            select.addEventListener('change', function () {
                if (select.value) {
                    modalInput.value = select.value;
                    modalSubmit();
                }
            });
        } catch (err) {
            console.error('Failed to load product types:', err);
        }
    })();

    // ── Contextual Nav — fade inactive buttons on flow ──
    window.activateFlow = function (flowName) {
        var btns = document.querySelectorAll('.qa-btn');
        btns.forEach(function (btn) {
            var btnText = btn.textContent.trim().toLowerCase();
            if (btnText.includes(flowName.toLowerCase())) {
                btn.classList.add('active-flow');
            } else {
                btn.classList.add('faded');
            }
        });
        document.getElementById('exitFlowBtn').classList.add('visible');
    };

    window.exitFlow = function () {
        var btns = document.querySelectorAll('.qa-btn');
        btns.forEach(function (btn) {
            btn.classList.remove('faded', 'active-flow');
        });
        document.getElementById('exitFlowBtn').classList.remove('visible');
    };

    // Hook into showModal to activate flow
    var origShowModal = window.showModal;
    window.showModal = function (type) {
        // Override compare to use selector panel
        if (type === 'compare') {
            activateFlow('Compare');
            openCompareSelector();
            return;
        }
        var flowNames = {
            'lookup': 'Lookup', 'chemical': 'Chemical', 'search': 'Search',
            'compare': 'Compare', 'manufacturer': 'Manufacturer',
            'product_type': 'Product Type', 'industry': 'Industry',
            'price': 'Price'
        };
        if (flowNames[type]) activateFlow(flowNames[type]);
        origShowModal(type);
    };

    // Exit flow on new chat
    var origNewChat2 = window.newChat;
    window.newChat = function () {
        exitFlow();
        origNewChat2();
    };

    // ── Session History ──
    var SEARCH_HISTORY_KEY = 'enpro_fm_search_history';
    var MAX_HISTORY = 30;

    function trackSearch(query, intent) {
        var history = JSON.parse(localStorage.getItem(SEARCH_HISTORY_KEY) || '[]');
        history.unshift({
            query: query,
            intent: intent || 'unknown',
            time: new Date().toISOString()
        });
        if (history.length > MAX_HISTORY) history = history.slice(0, MAX_HISTORY);
        localStorage.setItem(SEARCH_HISTORY_KEY, JSON.stringify(history));
        refreshHistorySidebar();
    }

    window.toggleHistorySidebar = function () {
        var sidebar = document.getElementById('historySidebar');
        sidebar.classList.toggle('open');
        if (sidebar.classList.contains('open')) {
            refreshHistorySidebar();
        }
    };

    function refreshHistorySidebar() {
        // Recent searches
        var searchesEl = document.getElementById('historySearches');
        var history = JSON.parse(localStorage.getItem(SEARCH_HISTORY_KEY) || '[]');

        if (history.length === 0) {
            searchesEl.innerHTML = '<div style="font-size:12px; color:var(--text-light); padding:6px 0;">No searches yet</div>';
        } else {
            var html = '';
            history.slice(0, 15).forEach(function (h) {
                var t = new Date(h.time);
                var timeStr = t.getHours() + ':' + String(t.getMinutes()).padStart(2, '0');
                html += '<div class="history-item" onclick="sendMessage(\'' + esc(h.query).replace(/'/g, "\\'") + '\')">';
                html += '<span class="history-item-query">' + esc(h.query) + '</span>';
                html += '<span class="history-item-time">' + timeStr + '</span>';
                html += '</div>';
            });
            searchesEl.innerHTML = html;
        }

        // Flagged reports
        var reportsEl = document.getElementById('historyReports');
        var reports = JSON.parse(localStorage.getItem('enpro_reports') || '[]');
        if (reports.length === 0) {
            reportsEl.innerHTML = '<div style="font-size:12px; color:var(--text-light); padding:6px 0;">No flagged reports</div>';
        } else {
            var rhtml = '';
            reports.forEach(function (r) {
                rhtml += '<div class="history-item">';
                rhtml += '<span class="history-item-query" style="color:var(--stock-red);">' + esc(r.part_number || r.partNumber || '') + '</span>';
                rhtml += '</div>';
            });
            reportsEl.innerHTML = rhtml;
        }

        // Session stats
        var statsEl = document.getElementById('historyStats');
        statsEl.innerHTML = '<div class="history-stat-row"><span class="history-stat-label">Queries</span><span class="history-stat-value">' + (adminStats ? adminStats.queries : 0) + '</span></div>'
            + '<div class="history-stat-row"><span class="history-stat-label">Cost</span><span class="history-stat-value">$' + (adminStats ? adminStats.cost.toFixed(3) : '0.000') + '</span></div>'
            + '<div class="history-stat-row"><span class="history-stat-label">Reports</span><span class="history-stat-value">' + reports.length + '</span></div>'
            + '<div class="history-stat-row"><span class="history-stat-label">Session</span><span class="history-stat-value">' + (sessionId ? sessionId.substring(0, 8) : '—') + '</span></div>';
    }

    window.emailReports = function () {
        var reports = JSON.parse(localStorage.getItem('enpro_reports') || '[]');
        if (reports.length === 0) {
            alert('No reports to email.');
            return;
        }

        fetch(API_BASE + '/api/email-report', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                subject: 'EnPro FM Portal — Flagged Reports (' + reports.length + ')',
                body: 'Reports flagged during session ' + (sessionId || 'unknown').substring(0, 8),
                reports: reports
            })
        })
        .then(function (r) { return r.json(); })
        .then(function (data) {
            if (data.status === 'sent') {
                alert('Reports emailed successfully.');
            } else if (data.status === 'queued') {
                alert('SMTP not configured yet. Reports saved to server queue.');
            } else {
                alert('Email failed: ' + (data.detail || 'Unknown error'));
            }
        })
        .catch(function () {
            alert('Could not send email. Check connection.');
        });
    };

    window.downloadTranscript = function () {
        var messages = chatArea.querySelectorAll('.msg');
        var lines = [];
        messages.forEach(function (msg) {
            var role = msg.classList.contains('user') ? 'USER' : 'BOT';
            var bubble = msg.querySelector('.msg-bubble');
            if (bubble) {
                lines.push('[' + role + '] ' + bubble.textContent.trim());
            }
        });

        var blob = new Blob([lines.join('\n\n')], { type: 'text/plain' });
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'enpro_session_' + (sessionId || 'unknown').substring(0, 8) + '.txt';
        a.click();
    };

    window.clearHistory = function () {
        if (confirm('Clear all search history?')) {
            localStorage.removeItem(SEARCH_HISTORY_KEY);
            refreshHistorySidebar();
        }
    };

    // ── Quote Builder ──
    var quoteData = {
        step: 0,
        company: '',
        contact_name: '',
        contact_email: '',
        contact_phone: '',
        ship_to: '',
        items: [],
        notes: ''
    };

    window.openQuoteModal = async function () {
        quoteData.step = 0;
        await fetchQuoteState();
        hydrateQuoteDataFromState();
        document.getElementById('quoteModalOverlay').classList.add('active');
        renderQuoteStep();
    };

    window.closeQuoteModal = function (e) {
        if (e && e.target !== document.getElementById('quoteModalOverlay')) return;
        document.getElementById('quoteModalOverlay').classList.remove('active');
    };

    function renderQuoteStep() {
        var body = document.getElementById('quoteModalBody');
        var prevBtn = document.getElementById('quotePrevBtn');
        var nextBtn = document.getElementById('quoteNextBtn');
        var titleEl = document.getElementById('quoteModalTitle');
        quoteData.items = getCombinedQuoteItems();

        var totalSteps = 3;

        // Step dots
        var dots = '<div class="quote-step-indicator">';
        for (var i = 0; i < totalSteps; i++) {
            var cls = i === quoteData.step ? 'active' : (i < quoteData.step ? 'done' : '');
            dots += '<div class="quote-step-dot ' + cls + '"></div>';
        }
        dots += '</div>';

        var html = dots;

        if (quoteData.step === 0) {
            titleEl.textContent = 'Customer Info';
            prevBtn.style.display = 'none';
            nextBtn.textContent = 'Next';

            html += '<div class="quote-field"><label>Company Name</label><input type="text" id="qCompany" value="' + esc(quoteData.company) + '" placeholder="e.g., Acme Corp"></div>';
            html += '<div class="quote-field"><label>Contact Name</label><input type="text" id="qName" value="' + esc(quoteData.contact_name) + '" placeholder="e.g., John Smith"></div>';
            html += '<div class="quote-field"><label>Email</label><input type="email" id="qEmail" value="' + esc(quoteData.contact_email) + '" placeholder="john@acme.com"></div>';
            html += '<div class="quote-field"><label>Phone</label><input type="tel" id="qPhone" value="' + esc(quoteData.contact_phone) + '" placeholder="(555) 123-4567"></div>';
            html += '<div class="quote-field"><label>Ship-to Location</label><input type="text" id="qShipTo" value="' + esc(quoteData.ship_to) + '" placeholder="City, State"></div>';

        } else if (quoteData.step === 1) {
            titleEl.textContent = 'Quote Items';
            prevBtn.style.display = '';
            nextBtn.textContent = 'Next';

            html += '<div id="quoteItemsList">';
            if (quoteData.items.length === 0) {
                html += '<div style="color:var(--text-light); font-size:13px; padding:8px 0;">No items captured yet. Keep talking or add a part number below.</div>';
            } else {
                quoteData.items.forEach(function (item, idx) {
                    html += '<div class="quote-item-row">';
                    html += '<span class="qi-pn">' + esc(item.part_number) + '</span>';
                    html += '<span class="qi-desc">' + esc(item.description || '') + '</span>';
                    html += '<span class="qi-qty"><input type="number" min="1" value="' + (item.quantity || 1) + '" onchange="updateQuoteItemQty(' + idx + ', this.value)"></span>';
                    html += '<button class="quote-item-remove" onclick="removeQuoteItem(' + idx + ')">&times;</button>';
                    html += '</div>';
                });
            }
            html += '</div>';
            html += '<div style="margin-top:10px; display:flex; gap:8px;">';
            html += '<input type="text" id="qAddPart" placeholder="Part number..." style="flex:1; padding:8px 12px; border:1px solid var(--border); border-radius:6px; font-size:14px;">';
            html += '<button class="quote-add-item" onclick="addQuoteItem()">+ Add</button>';
            html += '</div>';
            html += '<div class="quote-field" style="margin-top:14px;"><label>Notes</label><textarea id="qNotes" placeholder="Special instructions, quantities, etc.">' + esc(quoteData.notes) + '</textarea></div>';

        } else if (quoteData.step === 2) {
            titleEl.textContent = 'Review & Submit';
            prevBtn.style.display = '';
            nextBtn.textContent = 'Submit Quote';

            html += '<div class="quote-summary-section"><div class="quote-summary-label">Company</div><div class="quote-summary-value">' + esc(quoteData.company || '—') + '</div></div>';
            html += '<div class="quote-summary-section"><div class="quote-summary-label">Contact</div><div class="quote-summary-value">' + esc(quoteData.contact_name || '—') + ' &mdash; ' + esc(quoteData.contact_email || '') + ' ' + esc(quoteData.contact_phone || '') + '</div></div>';
            html += '<div class="quote-summary-section"><div class="quote-summary-label">Ship To</div><div class="quote-summary-value">' + esc(quoteData.ship_to || '—') + '</div></div>';

            html += '<div class="quote-summary-section"><div class="quote-summary-label">Items (' + quoteData.items.length + ')</div>';
            if (quoteData.items.length) {
                quoteData.items.forEach(function (item) {
                    html += '<div style="font-size:13px; padding:4px 0;">' + esc(item.part_number) + ' — Qty: ' + (item.quantity || 1) + (item.price ? ' — ' + esc(item.price) : '') + '</div>';
                });
            } else {
                html += '<div style="font-size:13px; color:var(--text-light);">No items</div>';
            }
            html += '</div>';

            if (quoteData.notes) {
                html += '<div class="quote-summary-section"><div class="quote-summary-label">Notes</div><div class="quote-summary-value">' + esc(quoteData.notes) + '</div></div>';
            }
        }

        body.innerHTML = html;
    }

    window.quoteStep = function (dir) {
        // Save current step data
        if (quoteData.step === 0) {
            var compEl = document.getElementById('qCompany');
            if (compEl) quoteData.company = compEl.value.trim();
            var nameEl = document.getElementById('qName');
            if (nameEl) quoteData.contact_name = nameEl.value.trim();
            var emailEl = document.getElementById('qEmail');
            if (emailEl) quoteData.contact_email = emailEl.value.trim();
            var phoneEl = document.getElementById('qPhone');
            if (phoneEl) quoteData.contact_phone = phoneEl.value.trim();
            var shipEl = document.getElementById('qShipTo');
            if (shipEl) quoteData.ship_to = shipEl.value.trim();
        } else if (quoteData.step === 1) {
            var notesEl = document.getElementById('qNotes');
            if (notesEl) quoteData.notes = notesEl.value.trim();
        }

        if (quoteData.step === 2 && dir === 1) {
            // Submit
            submitQuote();
            return;
        }

        quoteData.step = Math.max(0, Math.min(2, quoteData.step + dir));
        renderQuoteStep();
    };

    window.addQuoteItem = function () {
        var input = document.getElementById('qAddPart');
        if (!input || !input.value.trim()) return;
        var pn = input.value.trim();

        // Look up the part to get description and price
        fetch(API_BASE + '/api/lookup', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ part_number: pn, session_id: sessionId })
        })
        .then(function (r) { return r.json(); })
        .then(function (data) {
            var item = { part_number: pn, quantity: 1, description: '', price: '' };
            if (data.found && data.product) {
                item.description = data.product.Description || '';
                item.price = data.product.Price || '';
                item.part_number = data.product.Part_Number || pn;
            }
            quoteItems.push(item);
            localStorage.setItem('enpro_quote_items', JSON.stringify(quoteItems));
            if (data.quote_state) syncQuoteState(data.quote_state);
            hydrateQuoteDataFromState();
            renderQuoteStep();
        })
        .catch(function () {
            quoteItems.push({ part_number: pn, quantity: 1, description: '', price: '' });
            localStorage.setItem('enpro_quote_items', JSON.stringify(quoteItems));
            hydrateQuoteDataFromState();
            renderQuoteStep();
        });
    };

    window.removeQuoteItem = function (idx) {
        quoteData.items.splice(idx, 1);
        quoteItems = quoteData.items.map(function (item) {
            return {
                part_number: item.part_number,
                description: item.description || '',
                price: item.price || '',
                quantity: item.quantity || 1
            };
        });
        localStorage.setItem('enpro_quote_items', JSON.stringify(quoteItems));
        renderQuoteDrawer();
        renderQuoteStep();
    };

    window.updateQuoteItemQty = function (idx, val) {
        quoteData.items[idx].quantity = parseInt(val) || 1;
        quoteItems = quoteData.items.map(function (item) {
            return {
                part_number: item.part_number,
                description: item.description || '',
                price: item.price || '',
                quantity: item.quantity || 1
            };
        });
        localStorage.setItem('enpro_quote_items', JSON.stringify(quoteItems));
        renderQuoteDrawer();
    };

    function submitQuote() {
        hydrateQuoteDataFromState();
        fetch(API_BASE + '/api/quote', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                company: quoteData.company,
                contact_name: quoteData.contact_name,
                contact_email: quoteData.contact_email,
                contact_phone: quoteData.contact_phone,
                ship_to: quoteData.ship_to,
                items: quoteData.items,
                notes: quoteData.notes,
                session_id: sessionId
            })
        })
        .then(function (r) { return r.json(); })
        .then(function (data) {
            if (data.status === 'saved') {
                closeQuoteModal();
                appendMessage('bot', '<strong>Quote ' + esc(data.quote.id) + ' saved.</strong><br>EnPro will follow up within 1 business day.<br>Contact: service@enproinc.com | 1 (800) 323-2416');
                // Reset
                quoteData = { step: 0, company: '', contact_name: '', contact_email: '', contact_phone: '', ship_to: '', items: [], notes: '' };
                quoteItems = [];
                localStorage.removeItem('enpro_quote_items');
                renderQuoteDrawer();
            } else {
                alert('Quote save failed. Try again.');
            }
        })
        .catch(function () {
            alert('Could not save quote. Check connection.');
        });
    }

    // ── Ask John — Route through KB expertise ──
    window.askJohn = function () {
        var text = userInput.value.trim();

        // If input has text, route it through John's KB
        if (text) {
            userInput.value = '';
            userInput.style.height = 'auto';
            askJohnSend(text);
            return;
        }

        // If no text, look for context from the last product card on screen
        var cards = chatArea.querySelectorAll('.product-card');
        if (cards.length > 0) {
            var lastCard = cards[cards.length - 1];
            var header = lastCard.querySelector('.product-card-header');
            var partNumber = header ? header.textContent.replace('Part Number: ', '').trim() : '';
            if (partNumber) {
                askJohnSend('application advice for ' + partNumber);
                return;
            }
        }

        // No context — prompt user
        appendMessage('bot', 'Type a question or look up a product first, then hit <strong>Ask John</strong> to get expert KB guidance.');
    };

    async function askJohnSend(text) {
        if (isLoading) return;
        clearWelcome();
        appendMessage('user', '🔮 Ask John: ' + text);
        setLoading(true);
        var queryStart = Date.now();

        try {
            var res = await fetch(API_BASE + '/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: text, session_id: sessionId, mode: 'ask_john' })
            });
            var data = await res.json();
            handleResponse(data);
            trackQuery(queryStart, data);
            trackSearch('Ask John: ' + text, data.intent);
        } catch (err) {
            appendMessage('bot', 'Connection error. Please try again.');
            trackError();
        } finally {
            setLoading(false);
        }
    }

    // ── Two-Lane Workbench — Context Parser ──
    var sessionContext = {
        fluid: null,
        micron: null,
        temperature: null,
        flowRate: null,
        pressure: null,
        industry: null,
        chemical: null,
        pinnedPart: null,
        checkedChemical: false,
        compared: false
    };

    function parseAndUpdateContext(text) {
        var lower = text.toLowerCase();

        // Fluid detection
        var fluids = ['hydraulic oil', 'lube oil', 'sulfuric acid', 'hydrochloric acid',
            'acetic acid', 'acetone', 'water', 'glycol', 'amine', 'diesel', 'gasoline',
            'steam', 'compressed air', 'natural gas', 'crude oil', 'kerosene',
            'methanol', 'ethanol', 'caustic', 'bleach', 'brine'];
        for (var i = 0; i < fluids.length; i++) {
            if (lower.includes(fluids[i])) {
                sessionContext.fluid = fluids[i].replace(/\b\w/g, function(c) { return c.toUpperCase(); });
                break;
            }
        }

        // Micron
        var micronMatch = lower.match(/(\d+(?:\.\d+)?)\s*(?:micron|μm|um)\b/);
        if (micronMatch) sessionContext.micron = micronMatch[1] + ' \u03BCm';

        // Temperature
        var tempMatch = lower.match(/(\d{2,})\s*°?\s*(?:f|fahrenheit)/);
        if (tempMatch) sessionContext.temperature = tempMatch[1] + '\u00B0F';
        var tempC = lower.match(/(\d{2,})\s*°?\s*(?:c|celsius)/);
        if (tempC) sessionContext.temperature = tempC[1] + '\u00B0C';

        // Flow rate
        var flowMatch = lower.match(/(\d+(?:\.\d+)?)\s*(?:gpm|lpm)/i);
        if (flowMatch) sessionContext.flowRate = flowMatch[0].toUpperCase();

        // Pressure
        var psiMatch = lower.match(/(\d+)\s*(?:psi|bar)\b/i);
        if (psiMatch) sessionContext.pressure = psiMatch[0].toUpperCase();

        // Industry
        var industries = {
            'refinery': 'Refinery', 'pharmaceutical': 'Pharmaceutical', 'power plant': 'Power Plant',
            'semiconductor': 'Semiconductor', 'brewery': 'Brewery', 'beverage': 'Beverage',
            'dairy': 'Dairy', 'municipal': 'Municipal Water', 'chemical': 'Chemical Processing',
            'mining': 'Mining', 'wastewater': 'Wastewater', 'petrochemical': 'Petrochemical',
            'food': 'Food & Beverage', 'hydraulic': 'Hydraulic Systems', 'paint': 'Paint & Coatings'
        };
        for (var key in industries) {
            if (lower.includes(key)) {
                sessionContext.industry = industries[key];
                break;
            }
        }

        // Chemical (if asking about chemical compatibility)
        if (lower.includes('chemical') || lower.includes('compatibility')) {
            var chemNames = ['sulfuric acid', 'hydrochloric acid', 'acetone', 'methanol', 'ethanol',
                'caustic soda', 'bleach', 'ammonia', 'nitric acid', 'phosphoric acid', 'acetic acid',
                'toluene', 'xylene', 'mek', 'sodium hydroxide'];
            for (var c = 0; c < chemNames.length; c++) {
                if (lower.includes(chemNames[c])) {
                    sessionContext.chemical = chemNames[c].replace(/\b\w/g, function(ch) { return ch.toUpperCase(); });
                    sessionContext.checkedChemical = true;
                    break;
                }
            }
        }

        renderContextCard();
    }

    function renderContextCard() {
        var fields = [
            { id: 'ctxFluid', val: sessionContext.fluid },
            { id: 'ctxMicron', val: sessionContext.micron },
            { id: 'ctxTemp', val: sessionContext.temperature },
            { id: 'ctxFlow', val: sessionContext.flowRate },
            { id: 'ctxPSI', val: sessionContext.pressure },
            { id: 'ctxChemical', val: sessionContext.chemical },
        ];

        var filledCount = 0;
        fields.forEach(function (f) {
            var el = document.getElementById(f.id);
            if (!el) return;
            var valEl = el.querySelector('.ctx-value');
            if (f.val) {
                valEl.textContent = f.val;
                valEl.className = 'ctx-value ctx-filled';
                if (f.id !== 'ctxChemical') filledCount++;
            } else {
                valEl.textContent = f.id === 'ctxChemical' ? '\u2014' : '?';
                valEl.className = 'ctx-value ctx-empty';
            }
        });

        // Update spec count
        var countEl = document.getElementById('ctxSpecCount');
        if (countEl) countEl.textContent = filledCount + '/6';

        // Show quote readiness only when product pinned or 3+ specs filled
        var readinessEl = document.getElementById('ctxReadiness');
        if (readinessEl) {
            readinessEl.style.display = (sessionContext.pinnedPart || filledCount >= 3) ? '' : 'none';
        }

        // Update readiness steps
        var specStep = document.getElementById('ctxStepSpecs');
        if (specStep) {
            if (filledCount >= 3) {
                specStep.classList.add('done');
                specStep.querySelector('.ctx-check').innerHTML = '&#10003;';
            } else {
                specStep.classList.remove('done');
                specStep.querySelector('.ctx-check').innerHTML = '&#9675;';
            }
        }

        var prodStep = document.getElementById('ctxStepProduct');
        if (prodStep) {
            if (sessionContext.pinnedPart) {
                prodStep.classList.add('done');
                prodStep.querySelector('.ctx-check').innerHTML = '&#10003;';
            } else {
                prodStep.classList.remove('done');
                prodStep.querySelector('.ctx-check').innerHTML = '&#9675;';
            }
        }

        var chemStep = document.getElementById('ctxStepChemical');
        if (chemStep) {
            if (sessionContext.checkedChemical) {
                chemStep.classList.add('done');
                chemStep.querySelector('.ctx-check').innerHTML = '&#10003;';
            } else {
                chemStep.classList.remove('done');
                chemStep.querySelector('.ctx-check').innerHTML = '&#9675;';
            }
        }

        var compStep = document.getElementById('ctxStepCompare');
        if (compStep) {
            if (sessionContext.compared) {
                compStep.classList.add('done');
                compStep.querySelector('.ctx-check').innerHTML = '&#10003;';
            } else {
                compStep.classList.remove('done');
                compStep.querySelector('.ctx-check').innerHTML = '&#9675;';
            }
        }

        // Auto-expand lane 1 if it was collapsed and we have context
        if (filledCount > 0) {
            var lane1 = document.getElementById('lane1');
            if (lane1 && lane1.classList.contains('collapsed')) {
                lane1.classList.remove('collapsed');
            }
        }
    }

    function updateContextFromProducts(products) {
        if (!products || !products.length) return;
        var p = products[0]; // Use first product's specs
        if (p.Micron && !sessionContext.micron) sessionContext.micron = p.Micron + ' μm';
        if (p.Max_Temp_F && !sessionContext.temperature) sessionContext.temperature = p.Max_Temp_F + '°F';
        if (p.Max_PSI && !sessionContext.pressure) sessionContext.pressure = p.Max_PSI + ' PSI';
        if (p.Flow_Rate && !sessionContext.flowRate) sessionContext.flowRate = String(p.Flow_Rate);
        if (p.Media && !sessionContext.fluid && String(p.Media).toLowerCase() !== 'various') {
            // Don't overwrite fluid with media — different concept
        }
        if (p.Product_Type && !sessionContext.industry) {
            // Product type can hint at industry but don't overwrite
        }
        renderContextCard();
    }

    window.pinProduct = function (productData) {
        sessionContext.pinnedPart = productData;
        var el = document.getElementById('pinnedProduct');
        if (el && typeof window.renderProductCard === 'function') {
            el.innerHTML = '<div style="font-size:11px; text-transform:uppercase; color:var(--text-light); font-weight:700; margin-bottom:6px;">Pinned Product</div>' + window.renderProductCard(productData);
        }
        renderContextCard();
    };

    window.clearContext = function () {
        sessionContext = {
            fluid: null, micron: null, temperature: null, flowRate: null,
            pressure: null, industry: null, chemical: null, pinnedPart: null,
            checkedChemical: false, compared: false
        };
        var el = document.getElementById('pinnedProduct');
        if (el) el.innerHTML = '';
        renderContextCard();
    };

    window.toggleLane1 = function () {
        var lane1 = document.getElementById('lane1');
        var btn = document.getElementById('lane1Toggle');
        var expandTab = document.getElementById('lane1ExpandTab');
        if (lane1) {
            lane1.classList.toggle('collapsed');
            var isCollapsed = lane1.classList.contains('collapsed');
            if (btn) btn.innerHTML = isCollapsed ? '&#9654;' : '&#9664;';
            if (expandTab) expandTab.classList.toggle('visible', isCollapsed);
        }
    };

    // ── Quote Drawer (right side) ──
    var quoteItems = JSON.parse(localStorage.getItem('enpro_quote_items') || '[]');

    window.toggleQuoteDrawer = function () {
        var drawer = document.getElementById('quoteDrawer');
        if (drawer) drawer.classList.toggle('open');
    };

    window.addToQuote = function (btn) {
        var card = btn.closest('.product-card');
        if (!card) return;
        var header = card.querySelector('.product-card-header');
        var pn = header ? header.textContent.replace('Part Number: ', '').trim() : '';
        if (!pn) return;

        // Check duplicate across manual and conversational quote items
        if (getCombinedQuoteItems().some(function (q) { return q.part_number === pn; })) {
            btn.textContent = 'Already added';
            return;
        }

        var price = '';
        var priceEl = card.querySelector('.product-price');
        if (priceEl) price = priceEl.textContent.trim();

        var desc = '';
        var fields = card.querySelectorAll('.product-field');
        fields.forEach(function (f) {
            var label = f.querySelector('.product-field-label');
            if (label && label.textContent.trim() === 'Description') {
                desc = f.querySelector('.product-field-value').textContent.trim();
            }
        });

        quoteItems.push({ part_number: pn, description: desc, price: price, quantity: 1 });
        localStorage.setItem('enpro_quote_items', JSON.stringify(quoteItems));
        renderQuoteDrawer();

        btn.textContent = '✓ Added';
        btn.style.color = 'var(--stock-green)';
        btn.style.pointerEvents = 'none';
    };

    window.removeFromQuote = function (partNumber) {
        quoteItems = quoteItems.filter(function (item) { return item.part_number !== partNumber; });
        localStorage.setItem('enpro_quote_items', JSON.stringify(quoteItems));
        quoteData.items = getCombinedQuoteItems();
        renderQuoteDrawer();
        if (document.getElementById('quoteModalOverlay').classList.contains('active')) renderQuoteStep();
    };

    window.updateQuoteQty = function (partNumber, val) {
        var nextQty = parseInt(val) || 1;
        var manualItem = quoteItems.find(function (item) { return item.part_number === partNumber; });
        if (manualItem) {
            manualItem.quantity = nextQty;
        } else {
            quoteItems.push({ part_number: partNumber, description: '', price: '', quantity: nextQty });
        }
        localStorage.setItem('enpro_quote_items', JSON.stringify(quoteItems));
        quoteData.items = getCombinedQuoteItems();
        if (document.getElementById('quoteModalOverlay').classList.contains('active')) renderQuoteStep();
        renderQuoteDrawer();
    };

    function renderQuoteDrawer() {
        var body = document.getElementById('quoteDrawerBody');
        var countEl = document.getElementById('quoteItemCount');
        var tabEl = document.getElementById('quoteTab');
        var combinedItems = getCombinedQuoteItems();

        if (!body) return;

        if (combinedItems.length === 0) {
            body.innerHTML = '<div class="quote-drawer-empty">Talk through the quote or click "Add to Quote" on any product card to start building.</div>';
        } else {
            var html = '';
            combinedItems.forEach(function (item) {
                html += '<div class="quote-drawer-item">';
                html += '<div class="qdi-pn">' + esc(item.part_number) + '</div>';
                html += '<div class="qdi-price">' + esc(item.price || '') + '</div>';
                html += '<div class="qdi-qty"><input type="number" min="1" value="' + (item.quantity || 1) + '" onchange="updateQuoteQty(\'' + esc(item.part_number).replace(/'/g, "\\'") + '\', this.value)"></div>';
                html += '<button class="qdi-remove" onclick="removeFromQuote(\'' + esc(item.part_number).replace(/'/g, "\\'") + '\')">&times;</button>';
                html += '</div>';
            });
            body.innerHTML = html;
        }

        if (countEl) countEl.textContent = combinedItems.length;
        if (tabEl) tabEl.innerHTML = '&#128221; Quote (' + combinedItems.length + ')';
    }

    // Render on load
    renderQuoteDrawer();

    window.pinCardProduct = function(btn) {
        var card = btn.closest('.product-card');
        if (!card) return;
        var header = card.querySelector('.product-card-header');
        var pn = header ? header.textContent.replace('Part Number: ', '').trim() : '';
        // Extract fields from card DOM
        var fields = card.querySelectorAll('.product-field');
        var data = { Part_Number: pn };
        fields.forEach(function(f) {
            var label = f.querySelector('.product-field-label');
            var value = f.querySelector('.product-field-value');
            if (label && value) {
                var key = label.textContent.trim();
                if (key === 'Manufacturer') data.Final_Manufacturer = value.textContent.trim();
                if (key === 'Description') data.Description = value.textContent.trim();
                if (key === 'Product Type') data.Product_Type = value.textContent.trim();
            }
        });
        var price = card.querySelector('.product-price');
        if (price) data.Price = price.textContent.trim();
        pinProduct(data);
        btn.textContent = '\u2713 Pinned';
        btn.style.color = 'var(--stock-green)';
        btn.style.pointerEvents = 'none';
    };

    // ── Voice Agent (MediaRecorder → Server STT → Voice Search) ──
    var micBtn = document.getElementById('micBtn');
    var isListening = false;
    var voiceSynth = window.speechSynthesis;
    var lastInteractionWasVoice = false;
    var voiceMediaRecorder = null;
    var voiceAudioChunks = [];

    window.toggleVoice = function () {
        if (isListening) {
            stopListening();
        } else {
            startListening();
        }
    };

    async function startListening() {
        try {
            var stream = await navigator.mediaDevices.getUserMedia({
                audio: { noiseSuppression: true, echoCancellation: true, autoGainControl: true, channelCount: 1 }
            });
            voiceAudioChunks = [];
            var mimeType = MediaRecorder.isTypeSupported('audio/webm') ? 'audio/webm' : 'audio/mp4';
            voiceMediaRecorder = new MediaRecorder(stream, { mimeType: mimeType });

            voiceMediaRecorder.ondataavailable = function (e) {
                if (e.data.size > 0) voiceAudioChunks.push(e.data);
            };

            voiceMediaRecorder.onstop = async function () {
                stream.getTracks().forEach(function (t) { t.stop(); });
                if (voiceAudioChunks.length === 0) { return; }

                var blob = new Blob(voiceAudioChunks, { type: voiceMediaRecorder.mimeType });
                voiceAudioChunks = [];

                // Show processing state
                lastInteractionWasVoice = true;
                userInput.placeholder = 'Processing voice...';
                appendMessage('user', '🎤 Voice search...');

                try {
                    var formData = new FormData();
                    formData.append('file', blob, 'recording.webm');
                    var resp = await fetch(API_BASE + '/api/voice-search', { method: 'POST', body: formData });

                    if (!resp.ok) {
                        appendMessage('bot', 'Voice search failed. Try again or type your query.');
                        userInput.placeholder = 'Ask about a part, chemical, or product...';
                        return;
                    }

                    var data = await resp.json();

                    // Show what was heard
                    if (data.transcript) {
                        appendMessage('bot', '<em>I heard: "' + esc(data.transcript) + '"</em>');
                    }

                    // Show confidence suggestions ("Did you mean?")
                    if (data.suggestions && data.suggestions.length > 0) {
                        var sugHtml = '<div class="voice-suggestions" style="background:#fff3cd;padding:8px 12px;border-radius:8px;margin:4px 0;font-size:13px;">';
                        sugHtml += '<strong>Did you mean?</strong><br>';
                        data.suggestions.forEach(function (s) {
                            sugHtml += '<span style="color:#856404;">' + esc(s.field) + ': ';
                            sugHtml += '"' + esc(String(s.original)) + '" → <strong>' + esc(String(s.resolved)) + '</strong>';
                            sugHtml += ' (' + Math.round(s.confidence * 100) + '%)</span><br>';
                        });
                        sugHtml += '</div>';
                        appendMessage('bot', sugHtml);
                    }

                    // Render product results using existing card renderer
                    if (data.results && data.results.length > 0) {
                        var count = data.total_found || data.results.length;
                        appendMessage('bot', '<strong>' + count + ' product' + (count !== 1 ? 's' : '') + ' found</strong>' +
                            (data.filters_applied ? ' <span style="color:#666;font-size:12px;">(' + data.filters_applied.join(', ') + ')</span>' : ''));
                        data.results.forEach(function (product) {
                            appendCard(renderProductCard(product));
                        });
                    } else {
                        appendCard(renderVoiceFallbackCard(data.transcript || '', data));
                    }

                } catch (err) {
                    console.error('Voice search error:', err);
                    appendMessage('bot', 'Voice search failed. Try typing instead.');
                }

                userInput.placeholder = 'Ask about a part, chemical, or product...';
            };

            voiceMediaRecorder.start();
            isListening = true;
            micBtn.classList.add('listening');
            userInput.placeholder = 'Listening... click mic to stop';
        } catch (err) {
            console.error('Mic access error:', err);
            appendMessage('bot', 'Microphone access denied. Allow mic access in browser settings.');
        }
    }

    function stopListening() {
        isListening = false;
        micBtn.classList.remove('listening');
        userInput.placeholder = 'Ask about a part, chemical, or product...';
        if (voiceMediaRecorder && voiceMediaRecorder.state === 'recording') {
            voiceMediaRecorder.stop();
        }
    }

    // Text-to-Speech — read bot responses aloud
    function speakResponse(text) {
        if (!voiceSynth || !text) return;
        voiceSynth.cancel();

        var clean = text
            .replace(/\*\*/g, '')
            .replace(/\[V25 FILTERS\]/g, '')
            .replace(/\[NO PRICE\]/g, 'no price available')
            .replace(/\[NOT IN DATA\]/g, 'not in data')
            .replace(/#{1,3}\s/g, '')
            .replace(/\n/g, '. ')
            .replace(/\s+/g, ' ')
            .trim();

        if (clean.length > 500) {
            clean = clean.substring(0, 500) + '. See results above for full details.';
        }

        var utterance = new SpeechSynthesisUtterance(clean);
        utterance.lang = 'en-US';
        utterance.rate = 1.05;
        utterance.pitch = 1.0;

        var voices = voiceSynth.getVoices();
        var preferred = voices.find(function (v) {
            return v.name.includes('Google') || v.name.includes('Samantha') || v.name.includes('Zira');
        });
        if (preferred) utterance.voice = preferred;

        voiceSynth.speak(utterance);
    }

    // Hook: speak bot response when voice was used
    var origAppendMessage = appendMessage;
    appendMessage = function (role, html) {
        origAppendMessage(role, html);
        if (role === 'bot' && lastInteractionWasVoice) {
            var plainText = html.replace(/<[^>]+>/g, ' ').replace(/&[^;]+;/g, ' ').trim();
            speakResponse(plainText);
            lastInteractionWasVoice = false;
        }
    };

    // Preload voices (Chrome needs this)
    if (voiceSynth) {
        voiceSynth.getVoices();
        if (voiceSynth.onvoiceschanged !== undefined) {
            voiceSynth.onvoiceschanged = function () { voiceSynth.getVoices(); };
        }
    }

    // ── Simulate Mode — In-App Live Demo ──
    // Demo scenarios — grouped as FLOWS (3-4 chained commands per flow, no reset between)
    var SIM_SCENARIOS = [
        // FLOW 1: Full rep workflow — lookup → pin → chemical → compare
        { label: 'Part Lookup', query: '2004355', pause: 5000, flow: 1, autoPin: true,
          narration: 'Flow 1: Rep has a part number. Look it up, pin it, explore.' },
        { label: 'Chemical Check', query: 'chemical compatibility of sulfuric acid', pause: 7000, flow: 1,
          narration: 'Check chemical compatibility. A/B/C/D ratings for every material.' },
        { label: 'Find Alternative', query: 'CMBF1-30NN', pause: 5000, flow: 1,
          narration: 'Customer wants options. Pull up an alternative.' },
        { label: 'Compare Side-by-Side', query: 'compare 2004355 vs CMBF1-30NN', pause: 7000, flow: 1,
          narration: 'Side-by-side comparison. Real specs, real prices, real stock.' },

        // FLOW 2: Application branch → products → manufacturer
        { label: 'Brewery Branch', query: 'application brewery', pause: 7000, flow: 2,
          narration: 'Flow 2: Rep has a brewery call. Branch into application guidance.' },
        { label: 'Find Depth Sheets', query: 'search depth sheet', pause: 5000, flow: 2,
          narration: 'Prep says depth sheets. Search to find what is in stock.' },
        { label: 'Browse Filtrox', query: 'manufacturer Filtrox', pause: 5000, flow: 2,
          narration: 'Filtrox is the depth sheet brand. Browse their full catalog.' },
        { label: 'Check Acetone', query: 'chemical compatibility of acetone', pause: 7000, flow: 2,
          narration: 'Customer uses acetone for cleaning. Check compatibility fast.' },

        // FLOW 3: Natural language → context fills → expert
        { label: 'Describe the Need', query: 'I need a 10 micron polypropylene filter for hydraulic oil at 200F in a refinery', pause: 8000, flow: 3,
          narration: 'Flow 3: Customer describes what they need. Watch the context card fill.' },
        { label: 'Expert Advice', query: 'what filter for glycol dehydration in a gas plant', pause: 8000, flow: 3,
          narration: 'Ask the expert. 30 years of application knowledge, KB-backed.' },

        // FLOW 4: Safety + manufacturer browse
        { label: 'Browse PPC', query: 'manufacturer PPC', pause: 5000, flow: 4,
          narration: 'Flow 4: Browse a major manufacturer. See their full product line.' },
        { label: 'Safety Check', query: 'I need a filter for 500F hydrogen service', pause: 5000, flow: 4,
          narration: 'Safety guardrails. Dangerous conditions auto-escalate. No bad recommendations.' },
    ];

    var simState = { running: false, paused: false, step: 0, abortFlag: false };

    window.startSimulate = function () {
        if (simState.running) return;
        simState = { running: true, paused: false, step: 0, abortFlag: false };

        // Reset chat
        if (typeof newChat === 'function') newChat();

        // Create sim bar
        var bar = document.createElement('div');
        bar.className = 'sim-bar';
        bar.id = 'simBar';
        bar.innerHTML = '<div class="sim-bar-title">&#9654; LIVE DEMO</div>' +
            '<div class="sim-bar-step" id="simStep">Starting...</div>' +
            '<div class="sim-bar-progress"><div class="sim-bar-fill" id="simFill" style="width:0%"></div></div>' +
            '<div class="sim-bar-btns">' +
                '<button class="sim-bar-btn" id="simPauseBtn" onclick="simPause()">Pause</button>' +
                '<button class="sim-bar-btn" onclick="simSkip()">Skip</button>' +
                '<button class="sim-bar-btn" onclick="simExit()">Exit</button>' +
            '</div>';
        document.body.appendChild(bar);

        var narr = document.createElement('div');
        narr.className = 'sim-narration';
        narr.id = 'simNarration';
        narr.textContent = 'Watch how your team can find products, check compatibility, and prepare for calls in seconds.';
        document.body.appendChild(narr);

        // Offset content
        document.querySelector('.app-shell').style.marginTop = '90px';

        simSleep(3000).then(function () { runSimStep(0); });
    };

    function simSleep(ms) {
        return new Promise(function (resolve) {
            var start = Date.now();
            function check() {
                if (simState.abortFlag) { resolve(); return; }
                if (simState.paused) { setTimeout(check, 100); return; }
                if (Date.now() - start >= ms) { resolve(); return; }
                setTimeout(check, 50);
            }
            check();
        });
    }

    async function runSimStep(idx) {
        if (idx >= SIM_SCENARIOS.length || simState.abortFlag) {
            simFinish();
            return;
        }

        simState.step = idx;
        var scenario = SIM_SCENARIOS[idx];

        // Update bar
        var stepEl = document.getElementById('simStep');
        var fillEl = document.getElementById('simFill');
        var narrEl = document.getElementById('simNarration');
        if (stepEl) stepEl.textContent = 'Step ' + (idx + 1) + ' of ' + SIM_SCENARIOS.length + ': ' + scenario.label;
        if (fillEl) fillEl.style.width = ((idx / SIM_SCENARIOS.length) * 100) + '%';
        if (narrEl) narrEl.textContent = scenario.narration;

        // New chat only between FLOWS (not between chained steps within a flow)
        var prevFlow = idx > 0 ? SIM_SCENARIOS[idx - 1].flow : 0;
        if (idx > 0 && scenario.flow !== prevFlow && typeof newChat === 'function') newChat();
        await simSleep(500);
        if (simState.abortFlag) return;

        // Type query character by character
        userInput.value = '';
        userInput.focus();
        for (var i = 0; i < scenario.query.length; i++) {
            if (simState.abortFlag) return;
            await simSleep(50);
            userInput.value = scenario.query.substring(0, i + 1);
        }

        await simSleep(400);
        if (simState.abortFlag) return;

        // Submit
        handleSend();

        // Wait for response
        await simWaitForResponse();
        if (simState.abortFlag) return;

        // Update progress
        if (fillEl) fillEl.style.width = (((idx + 1) / SIM_SCENARIOS.length) * 100) + '%';

        // Scroll to see results
        scrollToBottom();

        // Auto-pin after first lookup in each flow (shows Lane 1 in action)
        if (scenario.autoPin) {
            await simSleep(1500);
            var pinBtns = document.querySelectorAll('.card-action-btn');
            for (var pb = 0; pb < pinBtns.length; pb++) {
                if (pinBtns[pb].textContent.includes('Pin')) {
                    pinBtns[pb].click();
                    var narrEl2 = document.getElementById('simNarration');
                    if (narrEl2) narrEl2.textContent = 'Product pinned to Lane 1. Context builds as the conversation continues.';
                    break;
                }
            }
        }

        // Pause for reading
        await simSleep(scenario.pause);
        if (simState.abortFlag) return;

        // Next step
        runSimStep(idx + 1);
    }

    function simWaitForResponse() {
        return new Promise(function (resolve) {
            var checks = 0;
            function poll() {
                if (simState.abortFlag) { resolve(); return; }
                if (!isLoading) { resolve(); return; }
                checks++;
                if (checks > 300) { resolve(); return; } // 30s timeout
                setTimeout(poll, 100);
            }
            setTimeout(poll, 500); // Give it a moment to start loading
        });
    }

    function simFinish() {
        simState.running = false;
        var fillEl = document.getElementById('simFill');
        if (fillEl) fillEl.style.width = '100%';
        var stepEl = document.getElementById('simStep');
        if (stepEl) stepEl.textContent = 'Demo Complete';
        var narrEl = document.getElementById('simNarration');
        if (narrEl) narrEl.textContent = '72,904 products. Real-time inventory. $0.02 per query. Ready for your team.';

        setTimeout(function () { simExit(); }, 5000);
    }

    window.simPause = function () {
        simState.paused = !simState.paused;
        var btn = document.getElementById('simPauseBtn');
        if (btn) btn.textContent = simState.paused ? 'Resume' : 'Pause';
    };

    window.simSkip = function () {
        if (simState.running && simState.step < SIM_SCENARIOS.length - 1) {
            simState.paused = false;
            simState.abortFlag = true;
            setTimeout(function () {
                simState.abortFlag = false;
                runSimStep(simState.step + 1);
            }, 100);
        }
    };

    window.simExit = function () {
        simState.abortFlag = true;
        simState.running = false;
        simState.paused = false;
        var bar = document.getElementById('simBar');
        if (bar) bar.remove();
        var narr = document.getElementById('simNarration');
        if (narr) narr.remove();
        document.querySelector('.app-shell').style.marginTop = '';
    };

    // Handle /simulate command in chat
    var origHandleSend = window.handleSend;
    window.handleSend = function () {
        var text = userInput.value.trim().toLowerCase();
        if (text === '/simulate' || text === 'simulate') {
            userInput.value = '';
            userInput.style.height = 'auto';
            startSimulate();
            return;
        }
        origHandleSend();
    };

    // URL param auto-start: ?simulate=true
    if (new URLSearchParams(window.location.search).get('simulate') === 'true') {
        setTimeout(startSimulate, 2000);
    }

})();

