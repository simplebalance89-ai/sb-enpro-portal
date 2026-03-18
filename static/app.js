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

        try {
            const res = await fetch(API_BASE + '/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: text, session_id: sessionId })
            });
            const data = await res.json();
            handleResponse(data);
        } catch (err) {
            appendMessage('bot', 'Connection error. Please check your network and try again.');
            console.error('Chat error:', err);
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
                var res = await fetch(API_BASE + '/api/suggest?q=' + encodeURIComponent(partNumber) + '&mode=' + mode);
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

    // Load products one at a time with a stagger delay for smooth cascading
    async function loadProductsStaggered(suggestions) {
        for (var i = 0; i < suggestions.length; i++) {
            try {
                var res = await fetch(API_BASE + '/api/lookup', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ part_number: suggestions[i].Part_Number })
                });
                var data = await res.json();
                if (data.found && data.product) {
                    appendCard(renderProductCard(data.product), true);
                }
            } catch (err) {
                console.error('Product fetch error for ' + suggestions[i].Part_Number + ':', err);
            }
            // Small delay between cards for smooth cascade effect
            if (i < suggestions.length - 1) {
                await new Promise(function (resolve) { setTimeout(resolve, 150); });
            }
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
            for (var i = 0; i < data.products.length; i++) {
                var p = data.products[i];
                appendCard(renderProductCard(p), data.products.length > 1);
                if (i === data.products.length - 1) appendFollowUps(p.Part_Number || p.part_number || '');
                if (data.products.length > 1 && i < data.products.length - 1) {
                    await new Promise(function (r) { setTimeout(r, 150); });
                }
            }
        } else if (data.results && Array.isArray(data.results) && data.results.length > 0) {
            if (data.total_found !== undefined) {
                var headerMsg = 'Found **' + data.total_found + '** products';
                if (data.total_found > data.results.length) headerMsg += ' (showing top ' + data.results.length + ')';
                headerMsg += ' [V25 FILTERS]:';
                appendMessage('bot', formatMarkdown(headerMsg));
            }
            for (var j = 0; j < data.results.length; j++) {
                var p2 = data.results[j];
                appendCard(renderProductCard(p2), data.results.length > 1);
                if (j === data.results.length - 1) appendFollowUps(p2.Part_Number || p2.part_number || '');
                if (data.results.length > 1 && j < data.results.length - 1) {
                    await new Promise(function (r) { setTimeout(r, 150); });
                }
            }
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
        html += '<div class="product-card-header">' + esc(String(pn)) + '</div>';
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

        // Footer
        html += '<div class="product-footer">';
        html += 'For additional information: <strong>EnPro Inc</strong> &mdash; ';
        html += '<a href="mailto:service@enproinc.com" style="color: var(--accent);">service@enproinc.com</a>';
        html += ' | <a href="tel:18003232416" style="color: var(--accent);">1 (800) 323-2416</a>';
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

    // ── Follow-up buttons ──
    function appendFollowUps(partNumber, customFollowUps) {
        if (customFollowUps) {
            // Use custom follow-ups as-is
            lastFollowUps = customFollowUps;
            var labels = customFollowUps;
        } else if (partNumber) {
            // Contextual follow-ups for a specific part
            lastFollowUps = [
                'chemical compatibility for ' + partNumber,
                'price ' + partNumber,
                'compare ' + partNumber,
                'manufacturer ' + partNumber,
                'quote ready'
            ];
            var labels = [
                'Chemical Check',
                'Price',
                'Compare',
                'Manufacturer',
                'Quote Ready'
            ];
        } else {
            return; // No part and no custom — skip
        }

        var container = document.createElement('div');
        container.className = 'followup-buttons';
        container.style.maxWidth = '85%';

        labels.forEach(function (label, i) {
            var btn = document.createElement('button');
            btn.className = 'followup-btn';
            btn.textContent = label;
            btn.onclick = function () { sendMessage(lastFollowUps[i]); };
            container.appendChild(btn);
        });

        chatArea.appendChild(container);
        scrollToBottom();
    }

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
            var res = await fetch(API_BASE + '/api/suggest?q=' + encodeURIComponent(query) + '&mode=' + mode);
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
