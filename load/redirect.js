import http from 'k6/http';
import { check } from 'k6';

const codes = JSON.parse(open('/load/codes.json'));

export const options = {
  scenarios: {
    redirects: {
      executor: 'constant-arrival-rate',
      rate: 700, timeUnit: '1s', duration: '2m',
      preAllocatedVUs: 100, maxVUs: 400,
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<50'],
    http_req_failed: ['rate<0.001'],
  },
};

export default function () {
  // skewed like real traffic: a small share of links gets most of the hits
  const i = Math.floor(codes.length * Math.pow(Math.random(), 3));
  const r = http.get(`${__ENV.BASE}/${codes[i]}`, { redirects: 0 });
  check(r, { '302': (x) => x.status === 302 });
}
