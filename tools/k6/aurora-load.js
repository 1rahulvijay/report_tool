import { check, sleep } from 'k6';
import http from 'k6/http';

/*
  Aurora Performance Load Test
  Simulates 200 concurrent users performing a realistic mix of metadata fetches,
  data previews, and export submissions.
*/

export const options = {
    stages: [
        { duration: '1m', target: 50 },  // Ramp up to 50 users
        { duration: '3m', target: 200 }, // Stay at 200 users
        { duration: '1m', target: 0 },   // Ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<2000'], // 95% of requests must complete below 2s
        http_req_failed: ['rate<0.01'],    // Less than 1% failures
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000/api/v1';

export default function () {
    // 1. Fetch Metadata (High frequency)
    const datasetsRes = http.get(`${BASE_URL}/datasets`);
    check(datasetsRes, { 'datasets fetched': (r) => r.status === 200 });

    sleep(Math.random() * 2 + 1);

    // 2. Data Preview (Medium frequency)
    // Hardcoded for testing with a known dataset
    const previewBody = JSON.stringify({
        dataset: 'AURORA_APP.EMPLOYEE',
        columns: ['ID', 'NAME', 'SALARY'],
        limit: 100
    });
    const previewRes = http.post(`${BASE_URL}/query/preview`, previewBody, {
        headers: { 'Content-Type': 'application/json' }
    });
    check(previewRes, { 'preview success': (r) => r.status === 200 || r.status === 429 });

    sleep(Math.random() * 5 + 2);

    // 3. Export Submission (Low frequency)
    if (Math.random() < 0.1) {
        const exportBody = JSON.stringify({
            dataset: 'AURORA_APP.EMPLOYEE',
            columns: ['ID', 'NAME', 'SALARY'],
            limit: 100000
        });
        const exportRes = http.post(`${BASE_URL}/query/export?format=excel`, exportBody, {
            headers: { 'Content-Type': 'application/json' }
        });
        // Success for async is 200 with job_id, or 429 for rate limit
        check(exportRes, { 'export submitted': (r) => r.status === 200 || r.status === 429 });
    }

    sleep(Math.random() * 10 + 5);
}
