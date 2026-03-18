const { test, expect } = require('@playwright/test');

const SITE = 'https://enpro-fm-portal.onrender.com';

// Wake up Render (cold start) before running tests
test.beforeAll(async ({ request }) => {
    console.log('Waking up Render service...');
    const res = await request.get(`${SITE}/health`);
    expect(res.ok()).toBeTruthy();
    const data = await res.json();
    console.log(`Health: ${data.status} | Products: ${data.product_count} | Chemicals: ${data.chemical_count}`);
});

test.describe('EnPro Filtration Mastermind — Full Test Suite', () => {

    test('01 — Homepage loads with quick actions', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await expect(page.locator('.welcome h2')).toContainText('Filtration Mastermind');
        // Verify all quick action buttons
        const buttons = page.locator('.qa-btn');
        await expect(buttons).toHaveCount(11); // Lookup, Chemical, Search, Compare, Manufacturer, Product Type, Industry, Pregame, Demo, Help, Build Quote
        await page.screenshot({ path: 'test-results/01-homepage.png', fullPage: true });
    });

    test('02 — Part Lookup: CLR10295', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        // Type directly into chat
        await page.fill('#userInput', 'CLR10295');
        await page.click('#sendBtn');
        // Wait for product card or response
        await page.waitForSelector('.msg.bot', { timeout: 30000 });
        await page.waitForTimeout(2000); // Let cards render
        await page.screenshot({ path: 'test-results/02-lookup-CLR10295.png', fullPage: true });
    });

    test('03 — Chemical Compatibility: Sulfuric Acid', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', 'chemical compatibility of sulfuric acid');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 45000 });
        await page.waitForTimeout(3000); // GPT response + card render
        await page.screenshot({ path: 'test-results/03-chemical-sulfuric-acid.png', fullPage: true });
    });

    test('04 — Chemical Compatibility: Acetone', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', 'chemical compatibility of acetone');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 45000 });
        await page.waitForTimeout(3000);
        await page.screenshot({ path: 'test-results/04-chemical-acetone.png', fullPage: true });
    });

    test('05 — Search: 10 micron filter cartridge', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', '10 micron filter cartridge');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 30000 });
        await page.waitForTimeout(2000);
        await page.screenshot({ path: 'test-results/05-search-10micron.png', fullPage: true });
    });

    test('06 — Manufacturer: Pall', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', 'manufacturer Pall');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 30000 });
        await page.waitForTimeout(2000);
        await page.screenshot({ path: 'test-results/06-manufacturer-pall.png', fullPage: true });
    });

    test('07 — Pregame: Brewery meeting', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', 'pregame brewery');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 45000 });
        await page.waitForTimeout(3000);
        await page.screenshot({ path: 'test-results/07-pregame-brewery.png', fullPage: true });
    });

    test('08 — Compare: CLR10295 vs CLR140', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', 'compare CLR10295 vs CLR140');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 30000 });
        await page.waitForTimeout(2000);
        await page.screenshot({ path: 'test-results/08-compare-clr130-vs-clr140.png', fullPage: true });
    });

    test('09 — Compare Side Panel: smart suggestions', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        // First look up a part to get action panel
        await page.fill('#userInput', 'CLR10295');
        await page.click('#sendBtn');
        await page.waitForSelector('.action-panel', { timeout: 30000 });
        await page.waitForTimeout(1000);
        // Click Compare in the action panel
        const compareBtn = page.locator('.action-card:has-text("Compare")');
        if (await compareBtn.isVisible()) {
            await compareBtn.click();
            await page.waitForSelector('.compare-panel.open', { timeout: 15000 });
            await page.waitForTimeout(3000); // API call for suggestions
            await page.screenshot({ path: 'test-results/09-compare-side-panel.png', fullPage: true });
        }
    });

    test('10 — Ask John button', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        // Type a question then hit Ask John
        await page.fill('#userInput', 'best filter for hydraulic oil at 10 micron');
        const askJohnBtn = page.locator('#askJohnBtn');
        if (await askJohnBtn.isVisible()) {
            await askJohnBtn.click();
            await page.waitForSelector('.msg.bot', { timeout: 45000 });
            await page.waitForTimeout(3000);
            await page.screenshot({ path: 'test-results/10-ask-john.png', fullPage: true });
        }
    });

    test('11 — History Sidebar', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        // Do a search first so history has content
        await page.fill('#userInput', 'CLR10295');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 30000 });
        await page.waitForTimeout(1000);
        // Open history sidebar
        const historyBtn = page.locator('#historyOpenBtn');
        if (await historyBtn.isVisible()) {
            await historyBtn.click();
            await page.waitForSelector('.history-sidebar.open', { timeout: 5000 });
            await page.waitForTimeout(500);
            await page.screenshot({ path: 'test-results/11-history-sidebar.png', fullPage: true });
        }
    });

    test('12 — Quote Builder modal', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        // Click Build Quote
        const quoteBtn = page.locator('.qa-btn:has-text("Build Quote")');
        if (await quoteBtn.isVisible()) {
            await quoteBtn.click();
            await page.waitForSelector('.quote-modal-overlay.active', { timeout: 5000 });
            await page.waitForTimeout(500);
            await page.screenshot({ path: 'test-results/12-quote-builder-step1.png', fullPage: true });
            // Fill step 1
            await page.fill('#qCompany', 'Acme Brewing Co.');
            await page.fill('#qName', 'John Smith');
            await page.fill('#qEmail', 'john@acmebrewing.com');
            await page.fill('#qPhone', '(555) 123-4567');
            await page.fill('#qShipTo', 'Houston, TX');
            await page.screenshot({ path: 'test-results/12-quote-builder-step1-filled.png', fullPage: true });
        }
    });

    test('13 — Industry quick action', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        // Click Industry button
        const industryBtn = page.locator('.qa-btn:has-text("Industry")');
        if (await industryBtn.isVisible()) {
            await industryBtn.click();
            await page.waitForSelector('.modal-overlay.active', { timeout: 5000 });
            await page.waitForTimeout(500);
            await page.screenshot({ path: 'test-results/13-industry-modal.png', fullPage: true });
        }
    });

    test('14 — Demo mode', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', 'demo');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 60000 });
        await page.waitForTimeout(5000); // Demo takes longer
        await page.screenshot({ path: 'test-results/14-demo-mode.png', fullPage: true });
    });

    test('15 — Help command', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        await page.fill('#userInput', 'help');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 15000 });
        await page.waitForTimeout(1000);
        await page.screenshot({ path: 'test-results/15-help.png', fullPage: true });
    });

    test('16 — Dark mode toggle', async ({ page }) => {
        await page.goto(SITE);
        await page.waitForSelector('.welcome', { timeout: 15000 });
        // Do a search first for visual content
        await page.fill('#userInput', 'CLR10295');
        await page.click('#sendBtn');
        await page.waitForSelector('.msg.bot', { timeout: 30000 });
        await page.waitForTimeout(2000);
        // Toggle dark mode
        const darkBtn = page.locator('#darkModeBtn');
        if (await darkBtn.isVisible()) {
            await darkBtn.click();
            await page.waitForTimeout(500);
            await page.screenshot({ path: 'test-results/16-dark-mode.png', fullPage: true });
        }
    });

    test('17 — Health API returns valid data', async ({ request }) => {
        const res = await request.get(`${SITE}/health`);
        expect(res.ok()).toBeTruthy();
        const data = await res.json();
        expect(data.status).toBe('healthy');
        expect(data.product_count).toBeGreaterThan(70000);
        expect(data.chemical_count).toBeGreaterThan(300);
        expect(data.azure_openai).toBeTruthy();
    });

    test('18 — Lookup API returns product', async ({ request }) => {
        const res = await request.post(`${SITE}/api/lookup`, {
            data: { part_number: 'CLR10295' }
        });
        expect(res.ok()).toBeTruthy();
        const data = await res.json();
        expect(data.found).toBeTruthy();
        expect(data.product.Part_Number).toBeTruthy();
    });

    test('19 — Compare Suggestions API', async ({ request }) => {
        const res = await request.post(`${SITE}/api/compare-suggestions`, {
            data: { part_number: 'CLR10295' }
        });
        expect(res.ok()).toBeTruthy();
        const data = await res.json();
        expect(data.source).toBeTruthy();
        expect(data.categories).toBeDefined();
    });

    test('20 — Manufacturers list API', async ({ request }) => {
        const res = await request.get(`${SITE}/api/manufacturers/list`);
        expect(res.ok()).toBeTruthy();
        const data = await res.json();
        expect(data.manufacturers.length).toBeGreaterThan(100);
    });

});
