/* ══════════════════════════════════════════════════════════════
   EnPro Filtration Mastermind — Embeddable Chat Widget

   Usage:
   <script src="https://enpro-fm-portal.azurewebsites.net/widget.js"></script>
   ══════════════════════════════════════════════════════════════ */

(function () {
    'use strict';

    // ── Config ──
    var WIDGET_ORIGIN = (function () {
        var scripts = document.getElementsByTagName('script');
        for (var i = scripts.length - 1; i >= 0; i--) {
            if (scripts[i].src && scripts[i].src.indexOf('widget.js') !== -1) {
                var url = new URL(scripts[i].src);
                return url.origin;
            }
        }
        return '';
    })();

    var CHAT_URL = WIDGET_ORIGIN + '/chat';

    // ── Styles ──
    var style = document.createElement('style');
    style.textContent = [
        '.enpro-widget-bubble {',
        '  position: fixed;',
        '  bottom: 24px;',
        '  right: 24px;',
        '  width: 60px;',
        '  height: 60px;',
        '  background: #003366;',
        '  border-radius: 50%;',
        '  cursor: pointer;',
        '  box-shadow: 0 4px 16px rgba(0,0,0,0.25);',
        '  display: flex;',
        '  align-items: center;',
        '  justify-content: center;',
        '  z-index: 999998;',
        '  transition: transform 0.2s, box-shadow 0.2s;',
        '}',
        '.enpro-widget-bubble:hover {',
        '  transform: scale(1.08);',
        '  box-shadow: 0 6px 20px rgba(0,0,0,0.3);',
        '}',
        '.enpro-widget-bubble svg {',
        '  width: 28px;',
        '  height: 28px;',
        '  fill: white;',
        '}',
        '.enpro-widget-bubble .enpro-close-icon { display: none; }',
        '.enpro-widget-bubble.open .enpro-chat-icon { display: none; }',
        '.enpro-widget-bubble.open .enpro-close-icon { display: block; }',
        '',
        '.enpro-widget-panel {',
        '  position: fixed;',
        '  bottom: 100px;',
        '  right: 24px;',
        '  width: 400px;',
        '  height: 600px;',
        '  background: white;',
        '  border-radius: 12px;',
        '  box-shadow: 0 8px 32px rgba(0,0,0,0.2);',
        '  z-index: 999997;',
        '  overflow: hidden;',
        '  transform: translateY(20px) scale(0.95);',
        '  opacity: 0;',
        '  pointer-events: none;',
        '  transition: transform 0.25s ease, opacity 0.25s ease;',
        '}',
        '.enpro-widget-panel.open {',
        '  transform: translateY(0) scale(1);',
        '  opacity: 1;',
        '  pointer-events: auto;',
        '}',
        '.enpro-widget-panel iframe {',
        '  width: 100%;',
        '  height: 100%;',
        '  border: none;',
        '}',
        '',
        '/* Tooltip */  ',
        '.enpro-widget-tooltip {',
        '  position: fixed;',
        '  bottom: 92px;',
        '  right: 28px;',
        '  background: #003366;',
        '  color: white;',
        '  padding: 8px 14px;',
        '  border-radius: 8px;',
        '  font-family: system-ui, sans-serif;',
        '  font-size: 13px;',
        '  font-weight: 500;',
        '  z-index: 999998;',
        '  box-shadow: 0 2px 8px rgba(0,0,0,0.2);',
        '  opacity: 0;',
        '  transform: translateY(4px);',
        '  transition: opacity 0.2s, transform 0.2s;',
        '  pointer-events: none;',
        '  white-space: nowrap;',
        '}',
        '.enpro-widget-tooltip::after {',
        '  content: "";',
        '  position: absolute;',
        '  bottom: -6px;',
        '  right: 20px;',
        '  width: 12px;',
        '  height: 12px;',
        '  background: #003366;',
        '  transform: rotate(45deg);',
        '}',
        '.enpro-widget-tooltip.show {',
        '  opacity: 1;',
        '  transform: translateY(0);',
        '}',
        '',
        '/* Mobile */  ',
        '@media (max-width: 480px) {',
        '  .enpro-widget-panel {',
        '    width: 100%;',
        '    height: 100%;',
        '    bottom: 0;',
        '    right: 0;',
        '    border-radius: 0;',
        '  }',
        '  .enpro-widget-bubble.open {',
        '    bottom: 16px;',
        '    right: 16px;',
        '    z-index: 999999;',
        '  }',
        '  .enpro-widget-tooltip { display: none; }',
        '}'
    ].join('\n');
    document.head.appendChild(style);

    // ── Bubble ──
    var bubble = document.createElement('div');
    bubble.className = 'enpro-widget-bubble';
    bubble.setAttribute('aria-label', 'Open EnPro chat');
    bubble.innerHTML = [
        '<svg class="enpro-chat-icon" viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H5.2L4 17.2V4h16v12z"/></svg>',
        '<svg class="enpro-close-icon" viewBox="0 0 24 24"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>'
    ].join('');

    // ── Tooltip ──
    var tooltip = document.createElement('div');
    tooltip.className = 'enpro-widget-tooltip';
    tooltip.textContent = 'Need filtration help?';

    // ── Panel ──
    var panel = document.createElement('div');
    panel.className = 'enpro-widget-panel';

    var iframe = document.createElement('iframe');
    iframe.title = 'EnPro Filtration Mastermind Chat';
    var iframeLoaded = false;

    panel.appendChild(iframe);

    // ── Append to DOM ──
    document.body.appendChild(panel);
    document.body.appendChild(tooltip);
    document.body.appendChild(bubble);

    // ── Toggle ──
    var isOpen = false;

    bubble.addEventListener('click', function () {
        isOpen = !isOpen;
        bubble.classList.toggle('open', isOpen);
        panel.classList.toggle('open', isOpen);
        tooltip.classList.remove('show');

        if (isOpen && !iframeLoaded) {
            iframe.src = CHAT_URL;
            iframeLoaded = true;
        }
    });

    // ── Show tooltip after 3s if not opened ──
    setTimeout(function () {
        if (!isOpen) {
            tooltip.classList.add('show');
            setTimeout(function () {
                tooltip.classList.remove('show');
            }, 5000);
        }
    }, 3000);

    // ── Hide tooltip on bubble hover ──
    bubble.addEventListener('mouseenter', function () {
        if (!isOpen) tooltip.classList.add('show');
    });
    bubble.addEventListener('mouseleave', function () {
        tooltip.classList.remove('show');
    });

})();
