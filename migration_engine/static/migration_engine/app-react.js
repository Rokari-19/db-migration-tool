(() => {
  const { createElement: h, useEffect, useState } = React;
  const apiBase = '/api/v1/migrations';

  async function api(url, opts = {}) {
    const response = await fetch(url, { headers: { 'Content-Type': 'application/json' }, ...opts });
    if (!response.ok) throw new Error(await response.text() || 'Request failed');
    return response.json();
  }

  function Layout({ children }) {
    return h('div', null,
      h('nav', { className: 'container-fluid' },
        h('ul', null, h('li', null, h('strong', null, 'Migration Assistant'))),
        h('ul', null,
          h('li', null, h('a', { href: '/' }, 'Dashboard')),
          h('li', null, h('a', { href: '/migrations/new/', role: 'button' }, 'New Migration'))
        )
      ),
      h('main', { className: 'container' }, children)
    );
  }

  function Dashboard() {
    const [jobs, setJobs] = useState([]); const [error, setError] = useState('');
    useEffect(() => { api(`${apiBase}/jobs`).then(setJobs).catch((e) => setError(e.message)); }, []);
    return h(Layout, null,
      h('h1', null, 'Database Migration Dashboard'),
      h('p', null, 'Built for non-technical teams.'),
      h('a', { href: '/migrations/new/', role: 'button' }, 'Start New Migration'),
      h('h2', null, 'Recent Jobs'),
      error ? h('p', null, `Could not load jobs: ${error}`) : h('table', null,
        h('thead', null, h('tr', null, h('th', null, 'Name'), h('th', null, 'Route'), h('th', null, 'Status'), h('th', null, 'Updated'), h('th', null, ''))),
        h('tbody', null, jobs.length ? jobs.map((j) => h('tr', { key: j.id },
          h('td', null, j.name),
          h('td', null, `${j.source_profile_name} → ${j.target_profile_name}`),
          h('td', null, j.status),
          h('td', null, new Date(j.updated_at).toLocaleString()),
          h('td', null, h('a', { href: `/migrations/${j.id}/` }, 'View'))
        )) : h('tr', null, h('td', { colSpan: 5 }, 'No jobs yet.')))
      )
    );
  }

  function Wizard() {
    const [step, setStep] = useState(1); const [plan, setPlan] = useState(null);
    const [conn, setConn] = useState({ source: { db_type: 'sqlite', database: '' }, target: { db_type: 'postgres', database: '' } });
    const [msg, setMsg] = useState('');
    const update = (side, key, value) => setConn((c) => ({ ...c, [side]: { ...c[side], [key]: value } }));

    async function testConnections() {
      const result = await api(`${apiBase}/connections/test`, { method: 'POST', body: JSON.stringify(conn) });
      setMsg(result.ok ? 'Connections look good.' : JSON.stringify(result));
      if (result.ok) setStep(2);
    }

    async function createPlan(e) {
      e.preventDefault();
      const fd = new FormData(e.target);
      const payload = {
        name: fd.get('name'), source_profile_id: Number(fd.get('source_profile_id')), target_profile_id: Number(fd.get('target_profile_id')),
        old_table: fd.get('old_table'), new_table: fd.get('new_table'), column_mapping: JSON.parse(fd.get('column_mapping') || '{}'),
        batch_size: Number(fd.get('batch_size')), stop_on_error: !!fd.get('stop_on_error')
      };
      const p = await api(`${apiBase}/plan`, { method: 'POST', body: JSON.stringify(payload) });
      setPlan(p); setStep(3);
    }

    async function run() { await api(`${apiBase}/${plan.job_id}/run`, { method: 'POST', body: '{}' }); setStep(4); }

    return h(Layout, null,
      h('h1', null, 'New Migration Wizard'),
      h('progress', { value: step, max: 4 }),
      step === 1 && h('article', null,
        h('h3', null, 'Step 1: Test Connections'),
        ['source', 'target'].map((s) => h('fieldset', { key: s },
          h('legend', null, s[0].toUpperCase() + s.slice(1)),
          h('input', { placeholder: 'database', value: conn[s].database, onChange: (e) => update(s, 'database', e.target.value) }),
          h('input', { placeholder: 'host', value: conn[s].host || '', onChange: (e) => update(s, 'host', e.target.value) }),
          h('input', { placeholder: 'username', value: conn[s].username || '', onChange: (e) => update(s, 'username', e.target.value) }),
          h('input', { placeholder: 'password', type: 'password', value: conn[s].password || '', onChange: (e) => update(s, 'password', e.target.value) })
        )),
        h('button', { onClick: testConnections }, 'Test Connections'), h('p', null, msg)
      ),
      step === 2 && h('form', { onSubmit: createPlan },
        h('h3', null, 'Step 2: Create Plan'),
        h('input', { name: 'name', placeholder: 'Migration Name', required: true }),
        h('input', { name: 'source_profile_id', type: 'number', placeholder: 'Source Profile ID', required: true }),
        h('input', { name: 'target_profile_id', type: 'number', placeholder: 'Target Profile ID', required: true }),
        h('input', { name: 'old_table', placeholder: 'Source table', required: true }),
        h('input', { name: 'new_table', placeholder: 'Target table', required: true }),
        h('textarea', { name: 'column_mapping', defaultValue: '{"id":"id"}' }),
        h('input', { name: 'batch_size', type: 'number', defaultValue: 500 }),
        h('label', null, h('input', { name: 'stop_on_error', type: 'checkbox', defaultChecked: true }), ' Stop on error'),
        h('button', { type: 'submit' }, 'Create Plan')
      ),
      step === 3 && h('article', null, h('h3', null, 'Step 3: Review'), h('p', null, `Job ID: ${plan.job_id}`), h('button', { onClick: run }, 'Start Migration')),
      step === 4 && h('article', null, h('h3', null, 'Step 4: Live Progress'), h('a', { href: `/migrations/${plan.job_id}/`, role: 'button' }, 'Open Job Details'))
    );
  }

  function Job({ jobId }) {
    const [status, setStatus] = useState(null); const [logs, setLogs] = useState([]);
    useEffect(() => {
      let timer;
      const refresh = async () => {
        const [s, l] = await Promise.all([api(`${apiBase}/${jobId}/status`), api(`${apiBase}/${jobId}/logs`)]);
        setStatus(s); setLogs(l.results || []);
        if (!['SUCCESS', 'FAILED'].includes(s.status)) timer = setTimeout(refresh, 2000);
      };
      refresh();
      return () => clearTimeout(timer);
    }, [jobId]);
    return h(Layout, null,
      h('h1', null, 'Migration Job Details'),
      !status ? h('p', null, 'Loading...') : h('article', null,
        h('h3', null, `${status.name} (#${status.id})`),
        h('p', null, `Status: ${status.status} | Stage: ${status.stage || 'n/a'}`),
        h('pre', null, JSON.stringify({ rows_read: status.rows_read, rows_written: status.rows_written, errors: status.errors }, null, 2))
      ),
      h('article', null, h('h3', null, 'Logs'), h('pre', null, logs.map((x) => `[${x.ts}] ${x.level} ${x.stage}: ${x.message}`).join('\n') || 'No logs yet'))
    );
  }

  const page = window.MIGRATION_CONTEXT?.page;
  const root = ReactDOM.createRoot(document.getElementById('root'));
  if (page === 'dashboard') root.render(h(Dashboard));
  else if (page === 'wizard') root.render(h(Wizard));
  else root.render(h(Job, { jobId: window.MIGRATION_CONTEXT?.jobId }));
})();
