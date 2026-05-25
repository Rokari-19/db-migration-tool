(() => {
  const { createElement: h, useEffect, useMemo, useState } = React;
  const apiBase = '/api/v1/migrations';

  const getToken = () => localStorage.getItem('auth_token') || '';
  const setTheme = (t) => document.documentElement.setAttribute('data-theme', t);

  async function api(url, opts = {}) {
    const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
    const token = getToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    const response = await fetch(url, { ...opts, headers });
    if (!response.ok) throw new Error(await response.text() || 'Request failed');
    return response.json();
  }

  function Layout({ children }) {
    const [theme, setThemeState] = useState(localStorage.getItem('theme') || 'light');
    useEffect(() => setTheme(theme), [theme]);
    const toggleTheme = () => { const n = theme === 'light' ? 'dark' : 'light'; setThemeState(n); localStorage.setItem('theme', n); };
    return h('div', null,
      h('nav', { className: 'container-fluid' },
        h('ul', null, h('li', null, h('strong', null, 'Migration Assistant'))),
        h('ul', null,
          h('li', null, h('a', { href: '/' }, 'Home')),
          h('li', null, h('a', { href: '/dashboard/' }, 'Dashboard')),
          h('li', null, h('a', { href: '/migrations/new/' }, 'New Migration')),
          h('li', null, h('a', { href: '/login/' }, 'Login')),
          h('li', null, h('button', { className: 'secondary', onClick: toggleTheme }, theme === 'light' ? 'Dark mode' : 'Light mode'))
        )
      ),
      h('main', { className: 'container' }, children)
    );
  }

  function Hero() {
    return h(Layout, null,
      h('section', { className: 'hero card' },
        h('h1', null, 'Simple, Safe Database Migrations'),
        h('p', null, 'Designed for non-technical teams with guided steps, clear checks, and live tracking.'),
        h('div', { className: 'hero-actions' },
          h('a', { href: '/migrations/new/', role: 'button' }, 'Start a Migration'),
          h('a', { href: '/dashboard/', role: 'button', className: 'secondary' }, 'View Dashboard')
        )
      )
    );
  }

  function Login() {
    const [msg, setMsg] = useState('');
    const submit = async (e) => {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        const res = await fetch('/dj-rest-auth/login/', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: fd.get('username'), password: fd.get('password') }) });
        const data = await res.json();
        if (!res.ok) throw new Error(JSON.stringify(data));
        localStorage.setItem('auth_token', data.access || data.key || '');
        setMsg('Login successful. You can now use authenticated API requests.');
      } catch (err) { setMsg(`Login failed: ${err.message}`); }
    };
    return h(Layout, null, h('article', { className: 'card' }, h('h2', null, 'Sign in'), h('form', { onSubmit: submit },
      h('input', { name: 'username', placeholder: 'Username', required: true }),
      h('input', { name: 'password', type: 'password', placeholder: 'Password', required: true }),
      h('button', { type: 'submit' }, 'Login')
    ), h('p', { className: 'helper' }, msg)));
  }

  function Dashboard() { const [jobs, setJobs] = useState([]); useEffect(() => { api(`${apiBase}/jobs`).then(setJobs).catch(() => setJobs([])); }, []);
    return h(Layout, null, h('h1', null, 'Dashboard'), h('a', { href: '/migrations/new/', role: 'button' }, 'Start New Migration'), h('table', null,
      h('thead', null, h('tr', null, h('th', null, 'Name'), h('th', null, 'Route'), h('th', null, 'Status'), h('th', null, 'Updated'))),
      h('tbody', null, jobs.length ? jobs.map((j) => h('tr', { key: j.id }, h('td', null, h('a', { href: `/migrations/${j.id}/` }, j.name)), h('td', null, `${j.source_profile_name} → ${j.target_profile_name}`), h('td', null, j.status), h('td', null, new Date(j.updated_at).toLocaleString()))) : h('tr', null, h('td', { colSpan: 4 }, 'No jobs yet.')))
    )); }

  function ConnectionPanel({ side, profiles, state, setState }) {
    const mode = state.mode || 'saved';
    const currentProfile = useMemo(() => profiles.find((p) => String(p.id) === String(state.profile_id)), [profiles, state.profile_id]);
    const assignFromProfile = (id) => {
      const p = profiles.find((x) => String(x.id) === String(id));
      if (!p) return;
      setState({ ...state, profile_id: id, db_type: p.db_type, database: p.database, host: p.host, port: p.port, username: p.username, password: p.password, uri: p.uri });
    };
    return h('fieldset', { className: 'card' },
      h('legend', null, side),
      h('label', null, h('input', { type: 'radio', checked: mode === 'saved', onChange: () => setState({ ...state, mode: 'saved' }) }), ' Use saved profile'),
      h('label', null, h('input', { type: 'radio', checked: mode === 'new', onChange: () => setState({ ...state, mode: 'new' }) }), ' Create new connection'),
      mode === 'saved' ? h('select', { value: state.profile_id || '', onChange: (e) => assignFromProfile(e.target.value) }, [h('option', { value: '', key: 'blank' }, 'Select profile')].concat(profiles.map((p) => h('option', { key: p.id, value: p.id }, `${p.name} (${p.db_type})`)))) : h('div', null,
        h('input', { placeholder: 'Profile name', value: state.name || '', onChange: (e) => setState({ ...state, name: e.target.value }) }),
        h('select', { value: state.db_type || 'sqlite', onChange: (e) => setState({ ...state, db_type: e.target.value }) }, h('option', { value: 'sqlite' }, 'SQLite'), h('option', { value: 'postgres' }, 'PostgreSQL'), h('option', { value: 'mongodb' }, 'MongoDB')),
        h('input', { placeholder: 'Database', value: state.database || '', onChange: (e) => setState({ ...state, database: e.target.value }) }),
        h('input', { placeholder: 'Host', value: state.host || '', onChange: (e) => setState({ ...state, host: e.target.value }) }),
        h('input', { placeholder: 'Port', value: state.port || '', onChange: (e) => setState({ ...state, port: e.target.value }) }),
        h('input', { placeholder: 'Username', value: state.username || '', onChange: (e) => setState({ ...state, username: e.target.value }) }),
        h('input', { type: 'password', placeholder: 'Password', value: state.password || '', onChange: (e) => setState({ ...state, password: e.target.value }) }),
        h('button', { className: 'secondary', onClick: async () => {
          const payload = { name: state.name, db_type: state.db_type, database: state.database, host: state.host || '', port: state.port ? Number(state.port) : null, username: state.username || '', password: state.password || '', uri: state.uri || '', ssl_mode: 'prefer' };
          const created = await api(`${apiBase}/profiles`, { method: 'POST', body: JSON.stringify(payload) });
          setState({ ...state, mode: 'saved', profile_id: created.id });
          window.location.reload();
        }, type: 'button' }, 'Save Profile')
      ),
      currentProfile && h('p', { className: 'helper' }, `Selected: ${currentProfile.name}`)
    );
  }

  function Wizard() {
    const [profiles, setProfiles] = useState([]); const [step, setStep] = useState(1); const [msg, setMsg] = useState(''); const [plan, setPlan] = useState(null);
    const [source, setSource] = useState({ mode: 'saved' }); const [target, setTarget] = useState({ mode: 'saved' });
    useEffect(() => { api(`${apiBase}/profiles`).then(setProfiles).catch(() => setProfiles([])); }, []);

    const buildConn = (s) => ({ db_type: s.db_type, database: s.database, host: s.host || '', port: s.port ? Number(s.port) : null, username: s.username || '', password: s.password || '', uri: s.uri || '', ssl_mode: 'prefer' });
    async function testConnections() {
      const result = await api(`${apiBase}/connections/test`, { method: 'POST', body: JSON.stringify({ source: buildConn(source), target: buildConn(target) }) });
      setMsg(result.ok ? '✅ Both connections are healthy.' : `❌ ${JSON.stringify(result)}`);
      if (result.ok) setStep(2);
    }

    return h(Layout, null, h('h1', null, 'Guided Migration Wizard'), h('progress', { value: step, max: 4 }),
      step === 1 && h('div', null, h('p', { className: 'helper' }, 'Pick saved profiles or create new connections.'), h('div', { className: 'form-grid' },
        h(ConnectionPanel, { side: 'Source', profiles, state: source, setState: setSource }),
        h(ConnectionPanel, { side: 'Target', profiles, state: target, setState: setTarget })
      ), h('button', { onClick: testConnections }, 'Test Connections'), h('p', null, msg)),
      step === 2 && h('form', { onSubmit: async (e) => {
        e.preventDefault(); const fd = new FormData(e.target);
        const p = await api(`${apiBase}/plan`, { method: 'POST', body: JSON.stringify({ name: fd.get('name'), source_profile_id: Number(source.profile_id), target_profile_id: Number(target.profile_id), old_table: fd.get('old_table'), new_table: fd.get('new_table'), column_mapping: JSON.parse(fd.get('column_mapping') || '{}'), batch_size: Number(fd.get('batch_size') || 500), stop_on_error: !!fd.get('stop_on_error') }) });
        setPlan(p); setStep(3);
      } },
      h('input', { name: 'name', placeholder: 'Migration name', required: true }), h('input', { name: 'old_table', placeholder: 'Source table', required: true }), h('input', { name: 'new_table', placeholder: 'Target table', required: true }), h('textarea', { name: 'column_mapping', defaultValue: '{"id":"id"}' }), h('input', { name: 'batch_size', type: 'number', defaultValue: 500 }), h('label', null, h('input', { type: 'checkbox', name: 'stop_on_error', defaultChecked: true }), ' Stop on error'), h('button', { type: 'submit' }, 'Create Plan')),
      step === 3 && h('article', { className: 'card' }, h('h3', null, 'Review'), h('p', null, `Job ID ${plan.job_id}`), h('button', { onClick: async () => { await api(`${apiBase}/${plan.job_id}/run`, { method: 'POST', body: '{}' }); setStep(4); } }, 'Start Migration')),
      step === 4 && h('article', { className: 'card' }, h('h3', null, 'Migration Started'), h('a', { href: `/migrations/${plan.job_id}/`, role: 'button' }, 'Open live status'))
    );
  }

  function Job({ jobId }) { const [status, setStatus] = useState(null); const [logs, setLogs] = useState([]);
    useEffect(() => { let t; const run = async () => { const [s,l] = await Promise.all([api(`${apiBase}/${jobId}/status`), api(`${apiBase}/${jobId}/logs`)]); setStatus(s); setLogs(l.results || []); if (!['SUCCESS','FAILED'].includes(s.status)) t = setTimeout(run, 2000); }; run(); return () => clearTimeout(t); }, [jobId]);
    return h(Layout, null, h('h1', null, 'Job Status'), status ? h('article', { className: 'card' }, h('p', null, `${status.name}: ${status.status}`), h('pre', null, JSON.stringify(status, null, 2))) : h('p', null, 'Loading...'), h('article', { className: 'card' }, h('h3', null, 'Logs'), h('pre', null, logs.map((x) => `[${x.ts}] ${x.level}: ${x.message}`).join('\n') || 'No logs')));
  }

  const page = window.MIGRATION_CONTEXT?.page; const root = ReactDOM.createRoot(document.getElementById('root'));
  if (page === 'hero') root.render(h(Hero));
  else if (page === 'login') root.render(h(Login));
  else if (page === 'dashboard') root.render(h(Dashboard));
  else if (page === 'wizard') root.render(h(Wizard));
  else root.render(h(Job, { jobId: window.MIGRATION_CONTEXT?.jobId }));
})();
