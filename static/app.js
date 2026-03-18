/* ══════════════════════════════════════════════════════════════
   EnPro Filtration Mastermind — Frontend App
   ══════════════════════════════════════════════════════════════ */

(function () {
    'use strict';

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

        clearWelcome();
        appendMessage('user', text);
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
                    appendMessage('bot', 'No products found ' + (mode === 'starts_with' ? 'starting with' : 'containing') + ' "' + esc(partNumber) + '".\nContact: service@enproinc.com | 1 (800) 323-2416');
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
                    body: JSON.stringify({ part_number: suggestions[0].Part_Number })
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

        // Handle different response shapes
        if (data.products && Array.isArray(data.products) && data.products.length > 0) {
            if (data.text || data.response) appendMessage('bot', formatMarkdown(data.text || data.response));
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
            appendFollowUps(data.product.part_number || '');
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
            appendMessage('bot', formatMarkdown('No products found matching "' + (data.query || '') + '".\nTry a different search term or contact EnPro.\nservice@enproinc.com | 1 (800) 323-2416'));
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
            '<div class="welcome-icon">&#9881;</div>' +
            '<h2>Welcome to Filtration Mastermind</h2>' +
            '<p>Your AI-powered filtration product assistant.<br>' +
            'Ask about parts, chemicals, specs, or use the quick actions above.</p>' +
            '</div>';
        // Reset state
        searchCount = 0;
        lastFollowUps = [];
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
        var html = '<div class="product-card">';
        html += '<div class="product-card-header">' + products.length + ' Results Found [V25 FILTERS]</div>';
        html += '<div class="product-card-body" style="padding:0;">';

        products.forEach(function (p, idx) {
            var pn = p.Part_Number || p.part_number || '?';
            var desc = p.Description || p.description || '';
            var mfr = p.Final_Manufacturer || p.Manufacturer || p.manufacturer || '';
            var price = p.Price || p.price || '';
            var stock = p.Total_Stock || p.total_stock || 0;
            var priceDisplay = (price && price !== 'Contact EnPro for pricing') ? price : '[NO PRICE]';
            var stockDisplay = stock > 0 ? '<span style="color:var(--stock-green);font-weight:700;">' + stock + ' in stock</span>' : '<span style="color:var(--stock-red);">Out of stock</span>';

            html += '<div class="consol-row" onclick="expandConsolRow(\'' + esc(pn) + '\', this)" style="cursor:pointer;">';
            html += '<div class="consol-row-main">';
            html += '<div class="consol-pn">' + esc(pn) + '</div>';
            html += '<div class="consol-desc">' + esc(desc) + '</div>';
            html += '</div>';
            html += '<div class="consol-row-meta">';
            if (mfr) html += '<span class="consol-mfr">' + esc(mfr) + '</span>';
            html += '<span class="consol-price">' + priceDisplay + '</span>';
            html += stockDisplay;
            html += '</div>';
            html += '</div>';
        });

        html += '</div></div>';
        return html;
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

        var fields = [
            ['Description', desc],
            ['Extended Desc', ext],
            ['Product Type', ptype],
            ['Manufacturer', mfg]
        ];

        // Specs line
        var specs = [];
        if (micron && micron !== '0' && micron !== '0.0') specs.push(micron + ' Micron');
        if (media) specs.push(media);
        if (tempF && tempF !== '0' && tempF !== '0.0') specs.push(tempF + '°F');
        if (psi && psi !== '0' && psi !== '0.0') specs.push(psi + ' PSI');
        if (flow) specs.push(flow);
        if (specs.length) fields.push(['Specs', specs.join(' | ')]);

        if (eff) fields.push(['Efficiency', eff]);

        fields.forEach(function (f) {
            if (f[1]) {
                html += '<div class="product-field">';
                html += '<div class="product-field-label">' + esc(f[0]) + '</div>';
                html += '<div class="product-field-value">' + esc(String(f[1])) + '</div>';
                html += '</div>';
            }
        });

        // Stock
        if (stock && typeof stock === 'object' && Object.keys(stock).length > 0) {
            html += '<div class="stock-section">';
            html += '<div class="stock-title">Inventory</div>';
            var locations = Object.keys(stock);
            var hasStock = false;
            locations.forEach(function (loc) {
                if (loc === 'status') {
                    html += '<div class="stock-row"><span style="color: var(--stock-red); font-weight: 600;">' + esc(String(stock[loc])) + '</span></div>';
                    return;
                }
                var qty = parseInt(stock[loc]) || 0;
                if (qty > 0) {
                    hasStock = true;
                    var badge = qty >= 10 ? 'green' : qty >= 3 ? 'orange' : 'red';
                    html += '<div class="stock-row">';
                    html += '<span>' + esc(loc) + '</span>';
                    html += '<span class="stock-qty ' + badge + '">' + qty + ' in stock</span>';
                    html += '</div>';
                }
            });
            if (!hasStock && !stock.status) {
                html += '<div class="stock-row"><span style="color: var(--stock-red); font-weight: 600;">Out of stock at all locations</span></div>';
            }
            html += '</div>';
        } else if (totalStock > 0) {
            html += '<div class="stock-section"><div class="stock-title">Inventory</div>';
            html += '<div class="stock-row"><span class="stock-qty green">' + totalStock + ' total in stock</span></div></div>';
        }

        // Price
        var priceStr = String(price);
        if (priceStr && priceStr !== '' && priceStr !== '0' && priceStr !== '0.0' && priceStr !== '$0.00') {
            if (priceStr.startsWith('$') || priceStr.startsWith('Contact')) {
                html += '<div class="product-price ' + (priceStr.startsWith('$') ? 'has-price' : 'no-price') + '">' + esc(priceStr) + '</div>';
            } else {
                var priceVal = parseFloat(priceStr);
                if (priceVal > 0) {
                    html += '<div class="product-price has-price">$' + priceVal.toFixed(2) + '</div>';
                } else {
                    html += '<div class="product-price no-price">Contact EnPro for pricing</div>';
                }
            }
        } else {
            html += '<div class="product-price no-price">Contact EnPro for pricing</div>';
        }

        html += '</div>'; // body

        // Card actions — print/copy
        html += '<div class="card-actions">';
        html += '<button class="card-action-btn" onclick="printCard(this)">&#128424; Print</button>';
        html += '<button class="card-action-btn" onclick="copyCard(this)">&#128203; Copy</button>';
        html += '<button class="card-action-btn" onclick="reportCard(this)" style="color:var(--stock-red);">&#9873; Report</button>';
        html += '</div>';

        // Footer — click to copy, no email app
        html += '<div class="product-footer">';
        html += '<strong>EnPro Inc</strong> &mdash; ';
        html += '<span class="copy-link" onclick="copyToClipboard(\'service@enproinc.com\', this)">service@enproinc.com</span>';
        html += ' | <span class="copy-link" onclick="copyToClipboard(\'1 (800) 323-2416\', this)">1 (800) 323-2416</span>';
        html += '</div>';

        html += '</div>';
        return html;
    };

    // ── Render chemical card ──
    window.renderChemicalCard = function (data) {
        var html = '<div class="chemical-card">';
        html += '<div class="chemical-card-header">' + esc(data.chemical || 'Chemical Compatibility') + '</div>';
        html += '<div class="chemical-card-body">';

        if (data.compatibilities && Array.isArray(data.compatibilities)) {
            data.compatibilities.forEach(function (row) {
                var status = (row.status || '').toLowerCase();
                var rowCls = status === 'compatible' ? 'compatible' :
                    status === 'not compatible' ? 'not-compatible' : 'limited';
                var badgeCls = status === 'compatible' ? 'green' :
                    status === 'not compatible' ? 'red' : 'orange';

                html += '<div class="compat-row ' + rowCls + '">';
                html += '<span><strong>' + esc(row.material || row.media || '') + '</strong></span>';
                html += '<span class="compat-badge ' + badgeCls + '">' + esc(row.status || 'Unknown') + '</span>';
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

        // Update quote tracker
        updateQuoteTracker('part', partNumber);

        // Build contextual action panel
        var panelId = 'actionPanel_' + Date.now();
        var panel = document.createElement('div');
        panel.className = 'msg bot';
        panel.innerHTML = '<div class="action-panel" id="' + panelId + '">' +
            '<div class="action-panel-header">Next Steps for ' + esc(partNumber) + '</div>' +
            '<div class="action-grid">' +
                '<div class="action-card" onclick="runAction(\'chemical\', \'' + esc(partNumber) + '\', this)">' +
                    '<div class="action-icon">&#9879;</div>' +
                    '<div class="action-label">Chemical Check</div>' +
                    '<div class="action-desc">Check material compatibility</div>' +
                '</div>' +
                '<div class="action-card" onclick="runAction(\'price\', \'' + esc(partNumber) + '\', this)">' +
                    '<div class="action-icon">&#128176;</div>' +
                    '<div class="action-label">Price</div>' +
                    '<div class="action-desc">Get current pricing</div>' +
                '</div>' +
                '<div class="action-card" onclick="showCompareForm(\'' + esc(partNumber) + '\', \'' + panelId + '\')">' +
                    '<div class="action-icon">&#9878;</div>' +
                    '<div class="action-label">Compare</div>' +
                    '<div class="action-desc">Side-by-side comparison</div>' +
                '</div>' +
                '<div class="action-card" onclick="runAction(\'manufacturer\', \'' + esc(partNumber) + '\', this)">' +
                    '<div class="action-icon">&#127981;</div>' +
                    '<div class="action-label">Manufacturer</div>' +
                    '<div class="action-desc">More from this brand</div>' +
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
        // Track in quote progress
        updateQuoteTracker(action, partNumber);

        switch (action) {
            case 'chemical':
                sendMessage('chemical compatibility for ' + partNumber);
                break;
            case 'price':
                sendMessage('price ' + partNumber);
                break;
            case 'manufacturer':
                sendMessage('manufacturer ' + partNumber);
                break;
        }
    };

    // Show inline compare form
    window.showCompareForm = function (partNumber, panelId) {
        var panel = document.getElementById(panelId);
        if (!panel) return;

        // Check if form already exists
        if (panel.querySelector('.compare-form')) return;

        var form = document.createElement('div');
        form.className = 'compare-form';
        form.innerHTML = '<div class="compare-form-inner">' +
            '<label>Compare <strong>' + esc(partNumber) + '</strong> with:</label>' +
            '<div style="display:flex; gap:8px; margin-top:6px;">' +
                '<input type="text" class="compare-input" id="compareInput_' + panelId + '" placeholder="Enter part number..." style="flex:1; padding:8px 12px; border:1px solid #ddd; border-radius:6px; font-size:14px;">' +
                '<button class="compare-go-btn" onclick="runCompare(\'' + esc(partNumber) + '\', \'' + panelId + '\')">Compare</button>' +
            '</div>' +
        '</div>';
        panel.appendChild(form);

        var input = document.getElementById('compareInput_' + panelId);
        input.focus();
        input.onkeydown = function (e) {
            if (e.key === 'Enter') {
                runCompare(partNumber, panelId);
            }
        };
        scrollToBottom();
    };

    // Execute the compare
    window.runCompare = function (partNumber, panelId) {
        var input = document.getElementById('compareInput_' + panelId);
        if (!input || !input.value.trim()) return;
        var compareTo = input.value.trim();
        updateQuoteTracker('compare', partNumber);
        sendMessage('compare ' + partNumber + ' vs ' + compareTo);
    };

    // ── Quote Readiness Tracker ──
    var quoteState = {
        part: null,
        price: false,
        chemical: false,
        compare: false,
        manufacturer: false
    };

    function updateQuoteTracker(action, partNumber) {
        if (action === 'part') {
            quoteState.part = partNumber;
            quoteState.price = false;
            quoteState.chemical = false;
            quoteState.compare = false;
            quoteState.manufacturer = false;
        } else {
            quoteState[action] = true;
        }
        renderQuoteTracker();
    }

    function renderQuoteTracker() {
        if (!quoteState.part) {
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

        var steps = [
            { key: 'part', label: 'Part', done: !!quoteState.part },
            { key: 'price', label: 'Price', done: quoteState.price },
            { key: 'chemical', label: 'Chemical', done: quoteState.chemical },
            { key: 'compare', label: 'Compare', done: quoteState.compare }
        ];

        var doneCount = steps.filter(function (s) { return s.done; }).length;
        var isReady = doneCount >= 3; // Part + 2 more = quote ready

        var html = '<div class="quote-tracker-inner">';
        html += '<div class="qt-title">Quote Readiness Tracker</div>';
        html += '<div class="quote-tracker-part">' + esc(quoteState.part) + '</div>';
        html += '<div class="quote-tracker-steps">';
        steps.forEach(function (step) {
            html += '<div class="qt-step ' + (step.done ? 'done' : '') + '">';
            html += '<span class="qt-check">' + (step.done ? '&#10003;' : '&#9675;') + '</span>';
            html += '<span class="qt-label">' + step.label + '</span>';
            html += '</div>';
        });
        html += '</div>';

        if (isReady) {
            html += '<button class="qt-ready-btn" onclick="sendMessage(\'quote ready for ' + esc(quoteState.part) + '\')">QUOTE READY</button>';
        } else {
            html += '<div class="qt-progress">' + doneCount + '/3 steps</div>';
        }

        html += '</div>';
        tracker.innerHTML = html;
    }

    // Reset quote tracker on new chat
    var origNewChat = window.newChat;
    window.newChat = function () {
        quoteState = { part: null, price: false, chemical: false, compare: false, manufacturer: false };
        adminStats = { queries: 0, cost: 0, totalLatency: 0, errors: 0, reports: 0 };
        updateAdminFooter();
        var tracker = document.getElementById('quoteTracker');
        if (tracker) tracker.remove();
        origNewChat();
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
        var s = esc(text);
        // Bold
        s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        // Italic
        s = s.replace(/\*(.+?)\*/g, '<em>$1</em>');
        // Inline code
        s = s.replace(/`(.+?)`/g, '<code>$1</code>');
        // Line breaks
        s = s.replace(/\n/g, '<br>');
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
        document.getElementById('pregameSelect').style.display = type === 'pregame' ? 'block' : 'none';
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
            case 'pregame':
                modalTitle.textContent = 'Meeting Pregame';
                modalLabel.textContent = 'Customer or Industry';
                modalInput.placeholder = 'e.g., brewery, refinery, municipal water';
                modalHint.textContent = 'Enter customer name or industry, or pick from the list below.';
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
            case 'pregame': sendMessage('pregame ' + val); break;
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

    // ── Pregame dropdown handler ──
    document.getElementById('pregameSelect').addEventListener('change', function () {
        if (this.value) {
            modalInput.value = this.value;
            modalSubmit();
        }
    });

    // ── Voice Agent (Speech-to-Text + Text-to-Speech) ──
    var micBtn = document.getElementById('micBtn');
    var recognition = null;
    var isListening = false;
    var voiceSynth = window.speechSynthesis;
    var lastInteractionWasVoice = false;

    var SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;

    window.toggleVoice = function () {
        if (!SpeechRecognition) {
            appendMessage('bot', 'Voice input is not supported in this browser. Try Chrome or Edge.');
            return;
        }
        if (isListening) {
            stopListening();
        } else {
            startListening();
        }
    };

    function startListening() {
        recognition = new SpeechRecognition();
        recognition.lang = 'en-US';
        recognition.interimResults = true;
        recognition.maxAlternatives = 1;
        recognition.continuous = false;

        isListening = true;
        lastInteractionWasVoice = true;
        micBtn.classList.add('listening');
        userInput.placeholder = 'Listening...';

        recognition.onresult = function (event) {
            var transcript = '';
            for (var i = event.resultIndex; i < event.results.length; i++) {
                transcript += event.results[i][0].transcript;
            }
            userInput.value = transcript;
            if (event.results[event.results.length - 1].isFinal) {
                stopListening();
                if (transcript.trim()) {
                    handleSend();
                }
            }
        };

        recognition.onerror = function (event) {
            console.error('Speech error:', event.error);
            stopListening();
            if (event.error === 'not-allowed') {
                appendMessage('bot', 'Microphone access denied. Allow mic access in browser settings.');
            }
        };

        recognition.onend = function () {
            stopListening();
        };

        try {
            recognition.start();
        } catch (err) {
            stopListening();
        }
    }

    function stopListening() {
        isListening = false;
        micBtn.classList.remove('listening');
        userInput.placeholder = 'Ask about a part, chemical, or product...';
        if (recognition) {
            try { recognition.stop(); } catch (e) {}
            recognition = null;
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

})();
