const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
    testDir: './tests',
    timeout: 60000,
    retries: 1,
    use: {
        baseURL: 'https://enpro-fm-portal.onrender.com',
        screenshot: 'on',
        video: 'off',
        trace: 'off',
    },
    reporter: [['html', { open: 'never' }], ['list']],
    projects: [
        {
            name: 'chromium',
            use: { browserName: 'chromium', viewport: { width: 1280, height: 900 } },
        },
        {
            name: 'mobile',
            use: { browserName: 'chromium', viewport: { width: 390, height: 844 } },
        },
    ],
    outputDir: './test-results',
});
