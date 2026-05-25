import { useState, useEffect } from "react";
import Papa from "papaparse";

const THRESHOLD = 0.0888;

function probToTier(prob) {
  if (prob < THRESHOLD) return { tier: "low", idx: 0 };
  if (prob < THRESHOLD * 2) return { tier: "moderate", idx: 1 };
  if (prob < THRESHOLD * 4) return { tier: "high", idx: 2 };
  return { tier: "critical", idx: 3 };
}

const TIER_STYLE = {
  low: { bg: "#EAF3DE", text: "#3B6D11", border: "#97C459", icon: "✓" },
  moderate: { bg: "#FAEEDA", text: "#854F0B", border: "#EF9F27", icon: "⚠" },
  high: { bg: "#FCEBEB", text: "#A32D2D", border: "#F09595", icon: "▲" },
  critical: { bg: "#FCEBEB", text: "#A32D2D", border: "#E24B4A", icon: "▲▲" },
};

const COMMUNITY_NEEDS = [
  {
    id: 1,
    priority: "sick",
    priorityLabel: "Priority 1 · Sick",
    name: "Pak Hendra, 58 — suspected dengue",
    detail: "Fever 3 days, no transport to clinic. Needs: ride to Puskesmas + check-in tomorrow.",
    needs: ["Car transport", "Check-in visit"],
  },
  {
    id: 2,
    priority: "sick",
    priorityLabel: "Priority 1 · Sick",
    name: "Ibu Sari, 34 — dengue recovery",
    detail: "Discharged yesterday, needs soft food & ORS. Single mother, 2 young children.",
    needs: ["Food / ORS", "Childcare help"],
  },
  {
    id: 3,
    priority: "elderly",
    priorityLabel: "Priority 2 · Elderly",
    name: "Mbah Sutarni, 72 — lives alone",
    detail: "Needs weekly check-in volunteer. Cannot empty water containers alone.",
    needs: ["Check-in visit", "Mosquito prevention"],
  },
  {
    id: 4,
    priority: "unemployed",
    priorityLabel: "Priority 2 · Unemployed",
    name: "Keluarga Eko — 4 people, no income",
    detail: "Recently laid off. Needs food assistance and mosquito net. No BPJS card.",
    needs: ["Food bank", "Mosquito net", "BPJS guidance"],
  },
  {
    id: 5,
    priority: "no_bpjs",
    priorityLabel: "Priority 3 · No BPJS",
    name: "Pak Dimas, 29 — freelancer",
    detail: "No health insurance, worried about treatment costs if sick.",
    needs: ["BPJS guidance"],
  },
];

const COMMUNITY_RESOURCES = [
  { id: 1, initials: "BW", name: "Budi Winarso", area: "RT 04", type: "Car / transport", availability: "Weekday afternoons", color: "#E1F5EE", textColor: "#0F6E56" },
  { id: 2, initials: "SH", name: "Siti Handayani", area: "RT 07", type: "Food bank + ORS", availability: "Saturday 9am–12pm", color: "#FAEEDA", textColor: "#854F0B" },
  { id: 3, initials: "DN", name: "Dr. Nurul Aini", area: "RT 02", type: "Medical advice", availability: "Online Q&A", color: "#E6F1FB", textColor: "#185FA5" },
  { id: 4, initials: "RJ", name: "Ratna Juwita", area: "RT 11", type: "Larvicide / prevention kits", availability: "Group buy coordinator", color: "#EEEDFE", textColor: "#3C3489" },
];

const PRIORITY_COLORS = {
  sick: { bg: "#FCEBEB", text: "#A32D2D", border: "#E24B4A" },
  elderly: { bg: "#FAEEDA", text: "#854F0B", border: "#EF9F27" },
  unemployed: { bg: "#FAEEDA", text: "#854F0B", border: "#EF9F27" },
  no_bpjs: { bg: "#E6F1FB", text: "#185FA5", border: "#85B7EB" },
};

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

async function callMatch(need, resource, riskLevel) {
  const res = await fetch(`${API_BASE}/api/match`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      need_name: need.name,
      need_detail: need.detail,
      need_priority: need.priority,
      resource_provider: resource.name,
      resource_type: resource.type,
      risk_level: riskLevel,
      language: "en",
    }),
  });
  if (!res.ok) throw new Error("API error");
  const data = await res.json();
  return data.result;
}

async function callAsk(question, riskLevel) {
  const res = await fetch(`${API_BASE}/api/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      question,
      context: `Indonesia, ${riskLevel} dengue risk`,
      risk_level: riskLevel,
    }),
  });
  if (!res.ok) throw new Error("API error");
  const data = await res.json();
  return data.result;
}

function MatchPanel({ need, resources, riskLevel, onClose }) {
  const [selectedResource, setSelectedResource] = useState(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  async function handleMatch() {
    if (!selectedResource) return;
    setLoading(true);
    setError(null);
    try {
      const text = await callMatch(need, selectedResource, riskLevel);
      setResult(text);
    } catch {
      setError("Could not reach the API. Make sure your backend is running.");
    } finally {
      setLoading(false);
    }
  }

  const pc = PRIORITY_COLORS[need.priority];

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.35)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ background: "#fff", borderRadius: 12, border: "0.5px solid #ddd", width: 480, maxWidth: "92vw", padding: "1.25rem", maxHeight: "85vh", overflowY: "auto" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
          <div>
            <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 6, background: pc.bg, color: pc.text, border: `0.5px solid ${pc.border}`, fontWeight: 500 }}>{need.priorityLabel}</span>
            <div style={{ fontSize: 14, fontWeight: 500, marginTop: 6 }}>{need.name}</div>
            <div style={{ fontSize: 12, color: "#666", marginTop: 2 }}>{need.detail}</div>
          </div>
          <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 18, cursor: "pointer", color: "#888", lineHeight: 1 }}>×</button>
        </div>
        <div style={{ borderTop: "0.5px solid #eee", paddingTop: 12, marginBottom: 12 }}>
          <div style={{ fontSize: 12, color: "#666", marginBottom: 8 }}>Choose a resource provider to match:</div>
          {resources.map((r) => (
            <div
              key={r.id}
              onClick={() => setSelectedResource(r)}
              style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 10px", borderRadius: 8, border: `0.5px solid ${selectedResource?.id === r.id ? "#1D9E75" : "#eee"}`, background: selectedResource?.id === r.id ? "#E1F5EE" : "transparent", cursor: "pointer", marginBottom: 6 }}
            >
              <div style={{ width: 32, height: 32, borderRadius: "50%", background: r.color, color: r.textColor, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 500, flexShrink: 0 }}>{r.initials}</div>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, fontWeight: 500 }}>{r.name} · {r.area}</div>
                <div style={{ fontSize: 11, color: "#666" }}>{r.type} · {r.availability}</div>
              </div>
              {selectedResource?.id === r.id && <span style={{ color: "#1D9E75", fontSize: 16 }}>✓</span>}
            </div>
          ))}
        </div>
        {!result && (
          <button
            onClick={handleMatch}
            disabled={!selectedResource || loading}
            style={{ width: "100%", padding: "9px", background: selectedResource ? "#1D9E75" : "#eee", color: selectedResource ? "#fff" : "#aaa", border: "none", borderRadius: 8, fontSize: 13, cursor: selectedResource ? "pointer" : "default", fontFamily: "inherit" }}
          >
            {loading ? "Generating coordination advice…" : "Generate match advice with AI"}
          </button>
        )}
        {error && <div style={{ fontSize: 12, color: "#A32D2D", marginTop: 8, padding: "8px 10px", background: "#FCEBEB", borderRadius: 6 }}>{error}</div>}
        {result && (
          <div style={{ marginTop: 12, padding: "12px 14px", background: "#E1F5EE", borderRadius: 8, border: "0.5px solid #97C459" }}>
            <div style={{ fontSize: 12, fontWeight: 500, color: "#0F6E56", marginBottom: 6 }}>AI coordination advice</div>
            <div style={{ fontSize: 13, color: "#085041", lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{result}</div>
            <button
              onClick={() => { setResult(null); setSelectedResource(null); }}
              style={{ marginTop: 10, fontSize: 11, padding: "3px 10px", borderRadius: 6, border: "0.5px solid #0F6E56", background: "none", cursor: "pointer", color: "#0F6E56", fontFamily: "inherit" }}
            >
              Try different match
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

function AskPanel({ riskLevel }) {
  const [question, setQuestion] = useState("");
  const [loading, setLoading] = useState(false);
  const [answer, setAnswer] = useState(null);
  const [error, setError] = useState(null);

  async function handleAsk() {
    if (!question.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const text = await callAsk(question, riskLevel);
      setAnswer(text);
    } catch {
      setError("Could not reach the API. Make sure your backend is running.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "1rem 1.25rem" }}>
      <div style={{ fontSize: 20, fontWeight: 700, marginBottom: 12 }}>Ask a health question</div>
      <textarea
        value={question}
        onChange={(e) => setQuestion(e.target.value)}
        placeholder="e.g. What should I do if I have fever and rash?"
        style={{ width: "100%", padding: "8px 10px", fontSize: 13, border: "0.5px solid #ddd", borderRadius: 8, minHeight: 100, resize: "vertical", fontFamily: "inherit", marginBottom: 8 }}
      />
      <button
        onClick={handleAsk}
        disabled={!question.trim() || loading}
        style={{ padding: "8px 18px", background: question.trim() ? "#185FA5" : "#eee", color: question.trim() ? "#fff" : "#aaa", border: "none", borderRadius: 8, fontSize: 13, cursor: question.trim() ? "pointer" : "default", fontFamily: "inherit" }}
      >
        {loading ? "Asking AI…" : "Ask"}
      </button>
      {error && <div style={{ fontSize: 12, color: "#A32D2D", marginTop: 8, padding: "8px 10px", background: "#FCEBEB", borderRadius: 6 }}>{error}</div>}
      {answer && (
        <div style={{ marginTop: 12, padding: "12px 14px", background: "#E6F1FB", borderRadius: 8, border: "0.5px solid #B5D4F4" }}>
          <div style={{ fontSize: 13, color: "#042C53", lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{answer}</div>
          <button
            onClick={() => { setAnswer(null); setQuestion(""); }}
            style={{ marginTop: 8, fontSize: 11, padding: "3px 10px", borderRadius: 6, border: "0.5px solid #185FA5", background: "none", cursor: "pointer", color: "#185FA5", fontFamily: "inherit" }}
          >
            Ask another question
          </button>
        </div>
      )}
    </div>
  );
}

export default function App() {
  const [predictions, setPredictions] = useState([]);
  const [countries, setCountries] = useState([]);
  const [weeks, setWeeks] = useState([]);
  const [selectedCountry, setSelectedCountry] = useState("INDONESIA");
  const [selectedWeek, setSelectedWeek] = useState("");
  const [predProb, setPredProb] = useState(0.05);
  const [riskIdx, setRiskIdx] = useState(0);
  const [role, setRole] = useState("resident");
  const [activePage, setActivePage] = useState("dashboard");
  const [matchNeed, setMatchNeed] = useState(null);

  const riskLevel = probToTier(predProb).tier;
  const isCritical = riskIdx >= 2;
  const ts = TIER_STYLE[riskLevel];

  useEffect(() => {
    Papa.parse("/test_predictions.csv", {
      download: true,
      header: true,
      complete: (results) => {
        const data = results.data.filter((r) => r.adm_0_name && r.pred_prob);
        setPredictions(data);
        const uniqueCountries = [...new Set(data.map((r) => r.adm_0_name))].sort();
        setCountries(uniqueCountries);
        const indonesiaWeeks = data
          .filter((r) => r.adm_0_name === "INDONESIA")
          .map((r) => r.iso_week)
          .filter((v, i, a) => a.indexOf(v) === i)
          .sort()
          .reverse();
        setWeeks(indonesiaWeeks);
        if (indonesiaWeeks.length > 0) {
          setSelectedWeek(indonesiaWeeks[0]);
          const row = data.find((r) => r.adm_0_name === "INDONESIA" && r.iso_week === indonesiaWeeks[0]);
          if (row) {
            const prob = parseFloat(row.pred_prob);
            setPredProb(prob);
            setRiskIdx(probToTier(prob).idx);
          }
        }
      },
    });
  }, []);

  function handleCountryChange(country) {
    setSelectedCountry(country);
    const countryWeeks = predictions
      .filter((r) => r.adm_0_name === country)
      .map((r) => r.iso_week)
      .filter((v, i, a) => a.indexOf(v) === i)
      .sort()
      .reverse();
    setWeeks(countryWeeks);
    if (countryWeeks.length > 0) {
      setSelectedWeek(countryWeeks[0]);
      const row = predictions.find((r) => r.adm_0_name === country && r.iso_week === countryWeeks[0]);
      if (row) {
        const prob = parseFloat(row.pred_prob);
        setPredProb(prob);
        setRiskIdx(probToTier(prob).idx);
      }
    }
  }

  function handleWeekChange(week) {
    setSelectedWeek(week);
    const row = predictions.find((r) => r.adm_0_name === selectedCountry && r.iso_week === week);
    if (row) {
      const prob = parseFloat(row.pred_prob);
      setPredProb(prob);
      setRiskIdx(probToTier(prob).idx);
    }
  }

  function navTo(page) {
    setActivePage(page);
  }

  function switchRole(r) {
    setRole(r);
    setActivePage(r === "resident" ? "dashboard" : "gov-dashboard");
  }

  const residentNav = [
    { id: "dashboard", icon: "🏠", label: "My community" },
    { id: "plaza", icon: "⊞", label: "Community plaza" },
    { id: "needs", icon: "♡", label: "Needs board" },
    { id: "resources", icon: "◫", label: "Share resources" },
    { id: "post", icon: "✎", label: "Post a request" },
    { id: "ask", icon: "?", label: "Ask AI" },
  ];

  const govNav = [
    { id: "gov-dashboard", icon: "▦", label: "Dashboard" },
    { id: "gov-allocation", icon: "◈", label: "Budget allocation" },
    { id: "gov-gaps", icon: "△", label: "Coverage gaps" },
  ];

  const navItems = role === "resident" ? residentNav : govNav;

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "system-ui, sans-serif", fontSize: 14 }}>
      {matchNeed && (
        <MatchPanel
          need={matchNeed}
          resources={COMMUNITY_RESOURCES}
          riskLevel={riskLevel}
          onClose={() => setMatchNeed(null)}
        />
      )}

      {/* sidebar */}
      <div style={{ width: 280, minWidth: 280, background: "#fff", borderRight: "0.5px solid #eee", padding: "1.75rem 1.5rem", display: "flex", flexDirection: "column" }}>

        {/* logo */}
        <div style={{ marginBottom: 4 }}>
          <div style={{ fontSize: 22, fontWeight: 700, color: "#111", letterSpacing: "-0.5px" }}>
            🦟 HealthRisk
          </div>
          <div style={{ fontSize: 12, color: "#888", marginTop: 2 }}>Community mutual aid</div>
        </div>

        <div style={{ height: 1, background: "#f0f0f0", margin: "14px 0" }} />

        {/* role toggle */}
        <div style={{ display: "flex", border: "0.5px solid #ddd", borderRadius: 10, overflow: "hidden", marginBottom: 14 }}>
          {["resident", "gov"].map((r) => (
            <button
              key={r}
              onClick={() => switchRole(r)}
              style={{ flex: 1, padding: "7px 0", fontSize: 13, background: role === r ? "#111" : "transparent", color: role === r ? "#fff" : "#666", border: "none", cursor: "pointer", fontWeight: role === r ? 600 : 400, fontFamily: "inherit" }}
            >
              {r === "resident" ? "Resident" : "Government"}
            </button>
          ))}
        </div>

        {/* risk badge */}
        <div style={{ padding: "10px 12px", borderRadius: 10, background: ts.bg, color: ts.text, border: `0.5px solid ${ts.border}`, marginBottom: 14 }}>
          <div style={{ fontSize: 11, opacity: 0.7, marginBottom: 2 }}>DENGUE RISK</div>
          <div style={{ fontSize: 18, fontWeight: 700, letterSpacing: "-0.3px" }}>
            {ts.icon} {riskLevel.charAt(0).toUpperCase() + riskLevel.slice(1)}
          </div>
          <div style={{ fontSize: 11, marginTop: 4, opacity: 0.8 }}>
            pred_prob: {predProb.toFixed(3)} · threshold: {THRESHOLD}
          </div>
        </div>

        {/* country selector */}
        <div style={{ marginBottom: 10 }}>
          <div style={{ fontSize: 11, color: "#aaa", marginBottom: 5, fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.5px" }}>District</div>
          <select
            value={selectedCountry}
            onChange={(e) => handleCountryChange(e.target.value)}
            style={{ width: "100%", padding: "7px 10px", fontSize: 13, border: "0.5px solid #ddd", borderRadius: 8, background: "#fafafa", color: "#111", fontFamily: "inherit", cursor: "pointer" }}
          >
            {countries.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>

        {/* week selector */}
        <div style={{ marginBottom: 14 }}>
          <div style={{ fontSize: 11, color: "#aaa", marginBottom: 5, fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.5px" }}>Week</div>
          <select
            value={selectedWeek}
            onChange={(e) => handleWeekChange(e.target.value)}
            style={{ width: "100%", padding: "7px 10px", fontSize: 13, border: "0.5px solid #ddd", borderRadius: 8, background: "#fafafa", color: "#111", fontFamily: "inherit", cursor: "pointer" }}
          >
            {weeks.map((w) => <option key={w} value={w}>{w}</option>)}
          </select>
        </div>

        <div style={{ height: 1, background: "#f0f0f0", marginBottom: 14 }} />

        {/* nav */}
        <div style={{ fontSize: 11, color: "#bbb", marginBottom: 8, textTransform: "uppercase", letterSpacing: ".5px", fontWeight: 500 }}>
          {role === "resident" ? "Resident" : "Government"}
        </div>
        {navItems.map((n) => (
          <button
            key={n.id}
            onClick={() => navTo(n.id)}
            style={{ display: "flex", alignItems: "center", gap: 9, padding: "8px 10px", borderRadius: 8, border: "none", background: activePage === n.id ? "#f5f5f5" : "transparent", cursor: "pointer", fontSize: 13, color: activePage === n.id ? "#111" : "#666", fontWeight: activePage === n.id ? 600 : 400, fontFamily: "inherit", textAlign: "left", marginBottom: 2 }}
          >
            <span style={{ fontSize: 15 }}>{n.icon}</span> {n.label}
          </button>
        ))}

        <div style={{ marginTop: "auto", paddingTop: 12, borderTop: "0.5px solid #eee" }}>
          <div style={{ fontSize: 11, color: "#ccc", marginBottom: 2 }}>LightGBM + Platt calibration</div>
          <div style={{ fontSize: 11, color: "#ccc" }}>ROC AUC 0.814 · F2 0.608</div>
        </div>
      </div>

      {/* main */}
      <div style={{ flex: 1, overflowY: "auto", padding: "2rem 3rem", background: "#fafafa" }}>

        {/* RESIDENT DASHBOARD */}
        {activePage === "dashboard" && (() => {
          const relativeRisk = (predProb / 0.1912).toFixed(2);
          const actionsByTier = {
            low: [
              { icon: "🪣", text: "Empty any standing water around your home — even bottle caps matter" },
              { icon: "🦟", text: "Use mosquito repellent when outdoors, especially at dawn and dusk" },
              { icon: "🌿", text: "Keep your surroundings clean — cut grass and remove debris" },
            ],
            moderate: [
              { icon: "🪣", text: "Check and empty ALL water containers within 500m urgently" },
              { icon: "💊", text: "Stock oral rehydration salts at home — available at Puskesmas" },
              { icon: "🩺", text: "If fever appears, go to clinic within 24 hours — do not wait" },
              { icon: "📢", text: "Tell your neighbors — community action cuts risk fastest" },
            ],
            high: [
              { icon: "🏥", text: "Go to Puskesmas immediately if you have fever + rash or muscle pain" },
              { icon: "💧", text: "Stay hydrated — drink at least 2L water daily if feeling unwell" },
              { icon: "🚨", text: "Call 119 ext 8 (Indonesia) if symptoms worsen rapidly" },
              { icon: "👴", text: "Check on elderly or sick neighbors today — they need you" },
            ],
            critical: [
              { icon: "🚨", text: "Seek medical care NOW if you have any fever — do not delay" },
              { icon: "📞", text: "Call 119 ext 8 immediately for emergency health assistance" },
              { icon: "🏥", text: "Nearest hospital: RSUD district hospital — bring BPJS card if available" },
              { icon: "🤝", text: "Post on the needs board if you cannot travel — community will help" },
            ],
          };
          const resourcesByCountry = {
            INDONESIA: [
              { type: "🏥", name: "Puskesmas (Community Health Center)", detail: "Free primary care · bring KTP", link: null },
              { type: "🏨", name: "RSUD District Hospital", detail: "Emergency & dengue ward · bring BPJS card", link: null },
              { type: "📞", name: "National Health Hotline", detail: "119 ext 8 · 24 hours", link: null },
              { type: "🌐", name: "World Mosquito Program", detail: "Wolbachia mosquito release program", link: "https://www.worldmosquitoprogram.org" },
            ],
            COLOMBIA: [
              { type: "🏥", name: "Centro de Salud", detail: "Primary care · bring cédula", link: null },
              { type: "📞", name: "Línea de Salud", detail: "106 · 24 hours", link: null },
              { type: "🌐", name: "INS Colombia", detail: "National Institute of Health", link: "https://www.ins.gov.co" },
            ],
            PERU: [
              { type: "🏥", name: "Centro de Salud MINSA", detail: "Free primary care · bring DNI", link: null },
              { type: "📞", name: "SAMU Peru", detail: "106 · emergency ambulance", link: null },
              { type: "🌐", name: "MINSA Peru", detail: "Ministry of Health · dengue alerts", link: "https://www.gob.pe/minsa" },
            ],
            BOLIVIA: [
              { type: "🏥", name: "Centro de Salud", detail: "Primary care · bring cédula", link: null },
              { type: "📞", name: "SEDES", detail: "Departmental health emergency line", link: null },
            ],
            PANAMA: [
              { type: "🏥", name: "CSS (Caja de Seguro Social)", detail: "Primary care · bring cédula", link: null },
              { type: "📞", name: "MINSA Panama", detail: "169 · health emergency", link: null },
              { type: "🌐", name: "MINSA Panama", detail: "Ministry of Health", link: "https://www.minsa.gob.pa" },
            ],
          };
          const supportLinks = [
            { icon: "❤️", label: "Donate to MSF", sub: "Dengue & tropical disease response", url: "https://www.msf.org/donate", color: "#FCEBEB", tc: "#A32D2D" },
            { icon: "🌍", label: "Support WHO", sub: "Global dengue elimination program", url: "https://www.who.int", color: "#E6F1FB", tc: "#185FA5" },
            { icon: "💼", label: "Health sector jobs", sub: "ReliefWeb · humanitarian roles", url: "https://reliefweb.int/jobs", color: "#EAF3DE", tc: "#3B6D11" },
            { icon: "🤝", label: "Volunteer", sub: "UN Volunteers · community health", url: "https://www.unv.org", color: "#FAEEDA", tc: "#854F0B" },
          ];
          const resources = resourcesByCountry[selectedCountry] || resourcesByCountry["INDONESIA"];
          const actions = actionsByTier[riskLevel];
          const recentRows = predictions
            .filter((r) => r.adm_0_name === selectedCountry)
            .sort((a, b) => b.iso_week.localeCompare(a.iso_week))
            .slice(0, 8)
            .reverse();
          const maxProb = Math.max(...recentRows.map((r) => parseFloat(r.pred_prob)), THRESHOLD * 4);
          const dengueFacts = [
            { icon: "🌡️", title: "Early symptoms", body: "Sudden high fever (39–40°C), severe headache, pain behind the eyes, muscle and joint pain. Symptoms appear 4–10 days after a mosquito bite." },
            { icon: "🦟", title: "How it spreads", body: "Transmitted only by the Aedes aegypti mosquito — not person to person. These mosquitoes bite during the day, especially at dawn and dusk." },
            { icon: "⚠️", title: "Warning signs", body: "Severe dengue can be life-threatening. Watch for: stomach pain, persistent vomiting, bleeding gums, fatigue — seek emergency care immediately." },
            { icon: "💉", title: "Treatment", body: "No specific antiviral treatment exists. Rest, fluids, and paracetamol for fever. Avoid ibuprofen and aspirin. Most people recover in 1–2 weeks." },
          ];
          return (
            <div>
              <div style={{ marginBottom: 28 }}>
                <div style={{ fontSize: 30, fontWeight: 800, color: "#111", letterSpacing: "-0.5px", marginBottom: 6 }}>
                  We're watching out for you 🤝
                </div>
                <div style={{ fontSize: 14, color: "#888" }}>
                  You're not alone in this.
                </div>
              </div>
              <div style={{ background: ts.bg, border: `1.5px solid ${ts.border}`, borderRadius: 16, padding: "20px 28px", marginBottom: 24 }}>
                <div style={{ fontSize: 11, color: ts.text, opacity: 0.65, marginBottom: 6, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.8px" }}>
                  {selectedCountry} · Week of {selectedWeek}
                </div>
                <div style={{ fontSize: 32, fontWeight: 800, color: ts.text, letterSpacing: "-0.5px" }}>
                  {ts.icon} {riskLevel.charAt(0).toUpperCase() + riskLevel.slice(1)} risk this week
                </div>
              </div>
              {recentRows.length > 1 && (
                <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "16px 20px", marginBottom: 24 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 10, color: "#111" }}>Recent trend — {selectedCountry}</div>
                  <div style={{ display: "flex", alignItems: "flex-end", gap: 4, height: 56 }}>
                    {recentRows.map((r, i) => {
                      const prob = parseFloat(r.pred_prob);
                      const h = Math.max(4, (prob / maxProb) * 56);
                      const tier = probToTier(prob).tier;
                      const barColor = TIER_STYLE[tier].border;
                      const isSelected = r.iso_week === selectedWeek;
                      return (
                        <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
                          <div style={{ width: "100%", height: h, background: barColor, borderRadius: 3, opacity: isSelected ? 1 : 0.45, outline: isSelected ? `2px solid ${barColor}` : "none" }} />
                          <div style={{ fontSize: 9, color: "#bbb", transform: "rotate(-45deg)", whiteSpace: "nowrap", marginTop: 2 }}>
                            {r.iso_week.slice(5)}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
              <div style={{ marginBottom: 20 }}>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 10, color: "#111" }}>Know dengue</div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
                  {dengueFacts.map((f, i) => (
                    <div key={i} style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "14px 16px" }}>
                      <div style={{ fontSize: 20, marginBottom: 6 }}>{f.icon}</div>
                      <div style={{ fontSize: 13, fontWeight: 600, color: "#111", marginBottom: 4 }}>{f.title}</div>
                      <div style={{ fontSize: 12, color: "#666", lineHeight: 1.6 }}>{f.body}</div>
                    </div>
                  ))}
                </div>
              </div>
              <div style={{ marginBottom: 20 }}>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 10, color: "#111" }}>What you can do today</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {actions.map((a, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "flex-start", gap: 12, padding: "12px 16px", background: "#fff", border: "0.5px solid #eee", borderRadius: 10 }}>
                    <span style={{ fontSize: 20 }}>{a.icon}</span>
                    <span style={{ fontSize: 13, color: "#333", lineHeight: 1.6 }}>{a.text}</span>
                  </div>
                ))}
                </div>
              </div>
              <div style={{ marginBottom: 24 }}>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 10, color: "#111" }}>Resources near you</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {resources.map((r, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 16px", background: "#fff", border: "0.5px solid #eee", borderRadius: 10 }}>
                    <span style={{ fontSize: 20 }}>{r.type}</span>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 13, fontWeight: 500, color: "#111" }}>{r.name}</div>
                      <div style={{ fontSize: 12, color: "#888" }}>{r.detail}</div>
                    </div>
                    {r.link && (
                      <a href={r.link} target="_blank" rel="noopener noreferrer" style={{ fontSize: 11, color: "#185FA5", textDecoration: "none", padding: "3px 8px", border: "0.5px solid #B5D4F4", borderRadius: 6 }}>
                        Visit →
                      </a>
                    )}
                  </div>
                ))}
                </div>
              </div>
              <div>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 4, color: "#111" }}>Make a difference</div>
                <div style={{ fontSize: 13, color: "#888", marginBottom: 12 }}>Donate, volunteer, or explore opportunities in global health</div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
                  {supportLinks.map((l, i) => (
                    <a key={i} href={l.url} target="_blank" rel="noopener noreferrer"
                      style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "12px 14px", background: l.color, borderRadius: 12, textDecoration: "none" }}>
                      <span style={{ fontSize: 22 }}>{l.icon}</span>
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 600, color: l.tc }}>{l.label}</div>
                        <div style={{ fontSize: 11, color: l.tc, opacity: 0.8, marginTop: 2, lineHeight: 1.4 }}>{l.sub}</div>
                      </div>
                    </a>
                  ))}
                </div>
              </div>
            </div>
          );
        })()}

        {/* PLAZA */}
        {activePage === "plaza" && (
          <div>
            <div style={{ fontSize: 20, fontWeight: 600, marginBottom: 4 }}>Community plaza</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: isCritical ? 12 : 20 }}>Share resources, ask for help, stay connected</div>
            {isCritical && (
              <div style={{ background: "#FCEBEB", border: "0.5px solid #F09595", borderRadius: 8, padding: "10px 14px", fontSize: 13, color: "#A32D2D", marginBottom: 16, display: "flex", gap: 8 }}>
                ▲ Outbreak risk is elevated. Check the needs board — urgent requests are prioritized.
              </div>
            )}
            {[
              { initials: "BW", color: "#E1F5EE", tc: "#0F6E56", name: "Budi Winarso", area: "RT 04", time: "2h ago", tag: "Has car", tagBg: "#EAF3DE", tagTc: "#3B6D11", body: "I can drive anyone who needs to get to Puskesmas this week — I have free afternoons. Especially happy to help elderly neighbors or anyone feeling unwell." },
              { initials: "SH", color: "#FAEEDA", tc: "#854F0B", name: "Siti Handayani", area: "RT 07", time: "5h ago", tag: "Food bank", tagBg: "#FAEEDA", tagTc: "#854F0B", body: "Our community food bank has extra rice, noodles, and oral rehydration salts. Families can pick up Saturday 9am–12pm at RT 07 balai. No ID required." },
              { initials: "DN", color: "#E6F1FB", tc: "#185FA5", name: "Dr. Nurul Aini", area: "RT 02", time: "1d ago", tag: "Medical", tagBg: "#E6F1FB", tagTc: "#185FA5", body: "I'm a nurse at RSUD. Happy to answer dengue questions here — when to go to hospital vs stay home, how to manage fever. Just reply or DM." },
              { initials: "RJ", color: "#EEEDFE", tc: "#3C3489", name: "Ratna Juwita", area: "RT 11", time: "1d ago", tag: "Prevention", tagBg: "#EEEDFE", tagTc: "#534AB7", body: "Group mosquito larvicide purchase — 20 bottles for Rp 180,000 split between households. Much cheaper than buying alone. DM before Friday." },
            ].map((p, i) => (
              <div key={i} style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "12px 14px", marginBottom: 10 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                  <div style={{ width: 36, height: 36, borderRadius: "50%", background: p.color, color: p.tc, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 500 }}>{p.initials}</div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 13, fontWeight: 500 }}>{p.name}</div>
                    <div style={{ fontSize: 11, color: "#aaa" }}>{p.time} · {p.area}</div>
                  </div>
                  <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 6, background: p.tagBg, color: p.tagTc }}>{p.tag}</span>
                </div>
                <div style={{ fontSize: 13, color: "#333", lineHeight: 1.6, marginBottom: 8 }}>{p.body}</div>
                <button style={{ fontSize: 12, padding: "3px 10px", borderRadius: 6, border: "0.5px solid #ddd", background: "none", cursor: "pointer", color: "#666", fontFamily: "inherit" }}>Reply</button>
              </div>
            ))}
          </div>
        )}

        {/* NEEDS BOARD */}
        {activePage === "needs" && (
          <div>
            <div style={{ fontSize: 20, fontWeight: 600, marginBottom: 4 }}>Needs board</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: 16 }}>Prioritized by vulnerability · click "Match" to get AI coordination advice</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
              <div>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 10 }}>Open needs</div>
                {COMMUNITY_NEEDS.map((n) => {
                  const pc = PRIORITY_COLORS[n.priority];
                  return (
                    <div key={n.id} style={{ borderLeft: `3px solid ${pc.border}`, background: "#fff", border: "0.5px solid #eee", borderLeftWidth: 3, borderLeftColor: pc.border, borderRadius: "0 8px 8px 0", padding: "10px 12px", marginBottom: 8 }}>
                      <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 6, background: pc.bg, color: pc.text, fontWeight: 500, display: "inline-block", marginBottom: 6 }}>{n.priorityLabel}</span>
                      <div style={{ fontSize: 13, fontWeight: 500, marginBottom: 2 }}>{n.name}</div>
                      <div style={{ fontSize: 12, color: "#666", marginBottom: 8 }}>{n.detail}</div>
                      <button onClick={() => setMatchNeed(n)} style={{ fontSize: 12, padding: "4px 12px", borderRadius: 6, border: "0.5px solid #ddd", background: "none", cursor: "pointer", color: "#333", fontFamily: "inherit" }}>
                        Match with resource →
                      </button>
                    </div>
                  );
                })}
              </div>
              <div>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 10 }}>Available resources</div>
                {COMMUNITY_RESOURCES.map((r) => (
                  <div key={r.id} style={{ display: "flex", alignItems: "flex-start", gap: 10, background: "#fff", border: "0.5px solid #eee", borderRadius: 8, padding: "10px 12px", marginBottom: 8 }}>
                    <div style={{ width: 36, height: 36, borderRadius: "50%", background: r.color, color: r.textColor, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 500, flexShrink: 0 }}>{r.initials}</div>
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 500 }}>{r.name} · {r.area}</div>
                      <div style={{ fontSize: 12, color: "#666" }}>{r.type}</div>
                      <div style={{ fontSize: 11, color: "#aaa" }}>{r.availability}</div>
                    </div>
                  </div>
                ))}
                <div style={{ background: "#f5f5f5", borderRadius: 8, padding: "10px 12px", marginTop: 8 }}>
                  <div style={{ fontSize: 12, color: "#666", marginBottom: 4 }}>Coverage summary</div>
                  <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13 }}>
                    <span style={{ color: "#666" }}>Community matched</span>
                    <span style={{ color: "#3B6D11", fontWeight: 500 }}>3 / 5 needs</span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginTop: 2 }}>
                    <span style={{ color: "#666" }}>Gap (needs gov)</span>
                    <span style={{ color: "#A32D2D", fontWeight: 500 }}>2 needs</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* SHARE RESOURCES */}
        {activePage === "resources" && (
          <div>
            <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 6 }}>Share resources</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: 20 }}>Tell the community what you can offer — your help could reach someone who needs it today.</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1.5rem", alignItems: "start" }}>
            <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "2rem" }}>
              {[
                { label: "Your name & location (RT/RW)", type: "text", ph: "e.g. Budi Winarso · RT 04 / RW 02" },
                { label: "Phone / contact (optional)", type: "text", ph: "e.g. 0812-xxxx-xxxx" },
              ].map((f, i) => (
                <div key={i} style={{ marginBottom: 12 }}>
                  <label style={{ fontSize: 12, color: "#666", display: "block", marginBottom: 4 }}>{f.label}</label>
                  <input type={f.type} placeholder={f.ph} style={{ width: "100%", padding: "10px 14px", fontSize: 14, border: "0.5px solid #ddd", borderRadius: 8, fontFamily: "inherit" }} />
                </div>
              ))}
              <div style={{ marginBottom: 12 }}>
                <label style={{ fontSize: 12, color: "#666", display: "block", marginBottom: 6 }}>What can you offer?</label>
                {["Car / transport", "Food / groceries", "Medical advice", "Mosquito repellent", "Spare bed / shelter", "Childcare", "Medication", "Other"].map((o) => (
                  <label key={o} style={{ display: "inline-flex", alignItems: "center", gap: 5, fontSize: 12, color: "#555", marginRight: 12, marginBottom: 6, cursor: "pointer" }}>
                    <input type="checkbox" /> {o}
                  </label>
                ))}
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ fontSize: 12, color: "#666", display: "block", marginBottom: 4 }}>Details & availability</label>
                <textarea placeholder="e.g. I can drive people to Puskesmas on weekday afternoons." style={{ width: "100%", padding: "7px 10px", fontSize: 13, border: "0.5px solid #ddd", borderRadius: 8, minHeight: 100, resize: "vertical", fontFamily: "inherit" }} />
              </div>
              <button style={{ padding: "11px 28px", background: "#1D9E75", color: "#fff", border: "none", borderRadius: 8, fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Post resource offer</button>
            </div>
            <div>
              <div style={{ background: "#EAF3DE", border: "0.5px solid #97C459", borderRadius: 12, padding: "1.5rem", marginBottom: 16 }}>
                <div style={{ fontSize: 16, fontWeight: 700, color: "#3B6D11", marginBottom: 8 }}>Why share?</div>
                <div style={{ fontSize: 13, color: "#3B6D11", lineHeight: 1.7 }}>
                  Community resources close gaps that government programs can't fill — faster, cheaper, and with more trust. A single car ride or bag of rice can change someone's week.
                </div>
              </div>
              <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "1.5rem" }}>
                <div style={{ fontSize: 14, fontWeight: 600, color: "#111", marginBottom: 12 }}>Most needed right now</div>
                {[
                  { icon: "🚗", label: "Car transport to clinic", count: "2 open requests" },
                  { icon: "🍚", label: "Food & ORS supplies", count: "1 open request" },
                  { icon: "👁️", label: "Check-in for elderly", count: "1 open request" },
                  { icon: "🦟", label: "Mosquito nets", count: "1 open request" },
                ].map((item, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 0", borderBottom: i < 3 ? "0.5px solid #f0f0f0" : "none" }}>
                    <span style={{ fontSize: 18 }}>{item.icon}</span>
                    <span style={{ flex: 1, fontSize: 13, color: "#333" }}>{item.label}</span>
                    <span style={{ fontSize: 11, color: "#A32D2D", background: "#FCEBEB", padding: "2px 8px", borderRadius: 6 }}>{item.count}</span>
                  </div>
                ))}
              </div>
            </div>
            </div>
          </div>
        )}

        {/* POST REQUEST */}
        {activePage === "post" && (
          <div>
            <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 6 }}>Post a request</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: 20 }}>Ask the community for help — requests are matched privately and with care.</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1.5rem", alignItems: "start" }}>
            <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "2rem" }}>
              <div style={{ marginBottom: 12 }}>
                <label style={{ fontSize: 12, color: "#666", display: "block", marginBottom: 4 }}>Your situation</label>
                <select style={{ width: "100%", padding: "10px 14px", fontSize: 14, border: "0.5px solid #ddd", borderRadius: 8, fontFamily: "inherit" }}>
                  <option>I or someone in my household is sick</option>
                  <option>I am elderly and live alone</option>
                  <option>I am unemployed / low income</option>
                  <option>I don&apos;t have BPJS / health insurance</option>
                  <option>I need food or basic supplies</option>
                  <option>I need transport to a clinic</option>
                  <option>Other</option>
                </select>
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ fontSize: 12, color: "#666", display: "block", marginBottom: 6 }}>What do you need?</label>
                {["Ride to clinic", "Food / ORS", "Medication", "Check-in visitor", "Mosquito net", "BPJS guidance", "Childcare"].map((o) => (
                  <label key={o} style={{ display: "inline-flex", alignItems: "center", gap: 5, fontSize: 12, color: "#555", marginRight: 12, marginBottom: 6, cursor: "pointer" }}>
                    <input type="checkbox" /> {o}
                  </label>
                ))}
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ fontSize: 12, color: "#666", display: "block", marginBottom: 4 }}>How urgent?</label>
                <select style={{ width: "100%", padding: "10px 14px", fontSize: 14, border: "0.5px solid #ddd", borderRadius: 8, fontFamily: "inherit" }}>
                  <option>Today / within hours</option>
                  <option>This week</option>
                  <option>Ongoing / recurring</option>
                </select>
              </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ fontSize: 12, color: "#666", display: "block", marginBottom: 4 }}>Additional details (optional)</label>
                <textarea placeholder="Location, urgency, number of people..." style={{ width: "100%", padding: "7px 10px", fontSize: 13, border: "0.5px solid #ddd", borderRadius: 8, minHeight: 100, resize: "vertical", fontFamily: "inherit" }} />
              </div>
              <button style={{ padding: "11px 28px", background: "#185FA5", color: "#fff", border: "none", borderRadius: 8, fontSize: 13, cursor: "pointer", fontFamily: "inherit" }}>Submit request</button>
            </div>
            <div>
              <div style={{ background: "#E6F1FB", border: "0.5px solid #B5D4F4", borderRadius: 12, padding: "1.5rem", marginBottom: 16 }}>
                <div style={{ fontSize: 16, fontWeight: 700, color: "#185FA5", marginBottom: 8 }}>You will be matched</div>
                <div style={{ fontSize: 13, color: "#185FA5", lineHeight: 1.7 }}>
                  Your request is shown anonymously on the needs board. Community members who can help will reach out. Government resources fill gaps when community supply runs out.
                </div>
              </div>
              <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "1.5rem" }}>
                <div style={{ fontSize: 14, fontWeight: 600, color: "#111", marginBottom: 12 }}>Priority order</div>
                {[
                  { icon: "🔴", label: "Sick or recovering", sub: "Matched first" },
                  { icon: "🟠", label: "Elderly living alone", sub: "Matched second" },
                  { icon: "🟡", label: "Unemployed / low income", sub: "Matched third" },
                  { icon: "🔵", label: "No BPJS / insurance", sub: "Guided to resources" },
                ].map((item, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 0", borderBottom: i < 3 ? "0.5px solid #f0f0f0" : "none" }}>
                    <span style={{ fontSize: 16 }}>{item.icon}</span>
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 500, color: "#111" }}>{item.label}</div>
                      <div style={{ fontSize: 11, color: "#888" }}>{item.sub}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
            </div>
          </div>
        )}

        {/* ASK AI */}
        {activePage === "ask" && (
          <div>
            <div style={{ fontSize: 20, fontWeight: 600, marginBottom: 4 }}>Ask a health question</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: 16 }}>Powered by Groq · llama-3.3-70b · responds in your language</div>
            <AskPanel riskLevel={riskLevel} />
          </div>
        )}

        {/* GOV DASHBOARD */}
        {activePage === "gov-dashboard" && (() => {
          const govMetrics = {
            low:      { openNeeds: 2, unmatched: 0, coverage: "85%", budgetLeft: "$48k", alert: null },
            moderate: { openNeeds: 3, unmatched: 1, coverage: "70%", budgetLeft: "$44k", alert: "1 high-priority need unmatched. Recommend: volunteer check-in program for elderly." },
            high:     { openNeeds: 5, unmatched: 2, coverage: "60%", budgetLeft: "$42k", alert: "2 high-priority needs unmatched. Government action recommended: BPJS emergency enrollment + volunteer check-in." },
            critical: { openNeeds: 8, unmatched: 4, coverage: "40%", budgetLeft: "$31k", alert: "CRITICAL: 4 needs unmatched. Deploy all available resources immediately. Activate emergency health response." },
          };
          const gm = govMetrics[riskLevel];
          return (
          <div>
            <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 6 }}>District overview</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: 20 }}>{selectedCountry} · Week of {selectedWeek} · {riskLevel.toUpperCase()} risk</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
              {[
                { label: "Open needs", value: gm.openNeeds.toString(), sub: `${gm.unmatched} unmatched`, vc: gm.unmatched > 0 ? "#A32D2D" : "#3B6D11" },
                { label: "Community resources", value: "4", sub: "active offers" },
                { label: "Coverage rate", value: gm.coverage, sub: "by community alone", vc: riskIdx >= 2 ? "#A32D2D" : "#3B6D11" },
                { label: "Gov budget left", value: gm.budgetLeft, sub: "of $50,000" },
              ].map((m, i) => (
                <div key={i} style={{ background: "#f5f5f5", borderRadius: 10, padding: "16px 18px" }}>
                  <div style={{ fontSize: 12, color: "#666", marginBottom: 6 }}>{m.label}</div>
                  <div style={{ fontSize: 26, fontWeight: 700, color: m.vc || "#111" }}>{m.value}</div>
                  <div style={{ fontSize: 11, color: "#aaa", marginTop: 2 }}>{m.sub}</div>
                </div>
              ))}
            </div>
            {gm.alert && (
            <div style={{ background: riskIdx >= 3 ? "#FCEBEB" : "#FAEEDA", border: `0.5px solid ${riskIdx >= 3 ? "#F09595" : "#EF9F27"}`, borderRadius: 8, padding: "12px 16px", fontSize: 13, color: riskIdx >= 3 ? "#A32D2D" : "#854F0B", marginBottom: 20 }}>
              {riskIdx >= 3 ? "🚨" : "⚠️"} {gm.alert}
            </div>
            )}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
              <div>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 10 }}>Vulnerability breakdown</div>
                <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "12px" }}>
                  {[
                    { label: "Sick / recovering", count: "2 people", status: "Matched", sc: "#3B6D11", dot: "#A32D2D" },
                    { label: "Elderly alone", count: "1 person", status: "Gap", sc: "#854F0B", dot: "#EF9F27" },
                    { label: "Unemployed / poor", count: "1 family", status: "Gap", sc: "#854F0B", dot: "#EF9F27" },
                    { label: "No BPJS", count: "1 person", status: "Guided", sc: "#3B6D11", dot: "#85B7EB" },
                  ].map((r, i) => (
                    <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 0", borderBottom: i < 3 ? "0.5px solid #eee" : "none" }}>
                      <span style={{ color: r.dot, fontSize: 16 }}>●</span>
                      <span style={{ flex: 1, fontSize: 13 }}>{r.label}</span>
                      <span style={{ fontSize: 12, color: "#888" }}>{r.count}</span>
                      <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 6, background: r.sc === "#3B6D11" ? "#EAF3DE" : "#FAEEDA", color: r.sc }}>{r.status}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div>
                <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 10 }}>Resource gaps by area</div>
                <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "12px" }}>
                  {[
                    { area: "RT 04", status: "Transport ✓", ok: true },
                    { area: "RT 07", status: "Food bank ✓", ok: true },
                    { area: "RT 02", status: "Medical ✓", ok: true },
                    { area: "RT 09, 12, 15", status: "No resources", ok: false },
                    { area: "RT 11", status: "Prevention only", ok: null },
                  ].map((r, i) => (
                    <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "7px 0", borderBottom: i < 4 ? "0.5px solid #eee" : "none", fontSize: 13 }}>
                      <span>{r.area}</span>
                      <span style={{ color: r.ok === true ? "#3B6D11" : r.ok === false ? "#A32D2D" : "#854F0B" }}>{r.status}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
          );
        })()}

        {/* GOV ALLOCATION */}
        {activePage === "gov-allocation" && (() => {
          const allocationByTier = {
            low: {
              used: 2000, total: 50000, pct: "4%",
              items: [
                { name: "Routine larvicide — high-density areas", impact: "Prevention", cost: "$1,200", badge: "Routine", bc: "#EAF3DE", bt: "#3B6D11" },
                { name: "Community health education sessions", impact: "Awareness", cost: "$800", badge: "Routine", bc: "#EAF3DE", bt: "#3B6D11" },
                { name: "Reserve — emergency response fund", impact: "–", cost: "$48,000", badge: "Reserve", bc: "#f5f5f5", bt: "#888" },
              ],
              note: "Low risk week — maintain routine prevention. Hold budget for potential escalation.",
            },
            moderate: {
              used: 5500, total: 50000, pct: "11%",
              items: [
                { name: "Targeted larvicide — RT 09, 12, 15", impact: "40% risk cut", cost: "$3,000", badge: "High impact", bc: "#EAF3DE", bt: "#3B6D11" },
                { name: "Volunteer check-in — elderly residents", impact: "5 elderly", cost: "$1,800", badge: "High impact", bc: "#EAF3DE", bt: "#3B6D11" },
                { name: "Mosquito net distribution", impact: "12 families", cost: "$700", badge: "Medium", bc: "#FAEEDA", bt: "#854F0B" },
                { name: "Reserve", impact: "–", cost: "$44,500", badge: "Reserve", bc: "#f5f5f5", bt: "#888" },
              ],
              note: "Moderate risk — focus on gap areas. Monitor for escalation.",
            },
            high: {
              used: 8000, total: 50000, pct: "16%",
              items: [
                { name: "Emergency BPJS enrollment — Keluarga Eko", impact: "Covers 4 people", cost: "$200", badge: "Urgent", bc: "#FCEBEB", bt: "#A32D2D" },
                { name: "Volunteer check-in — elderly in RT 09, 12, 15", impact: "5 elderly", cost: "$1,800", badge: "High impact", bc: "#EAF3DE", bt: "#3B6D11" },
                { name: "Community-wide larvicide spraying", impact: "60% risk cut", cost: "$6,000", badge: "High impact", bc: "#EAF3DE", bt: "#3B6D11" },
                { name: "Mosquito net distribution — low income households", impact: "18 families", cost: "$900", badge: "Medium", bc: "#FAEEDA", bt: "#854F0B" },
                { name: "Reserve — new needs this week", impact: "–", cost: "$42,100", badge: "Reserve", bc: "#f5f5f5", bt: "#888" },
              ],
              note: "High risk — deploy gap-filling resources. Do not duplicate community coverage.",
            },
            critical: {
              used: 19000, total: 50000, pct: "38%",
              items: [
                { name: "Emergency hospital capacity — RSUD surge support", impact: "Critical", cost: "$8,000", badge: "🚨 Critical", bc: "#FCEBEB", bt: "#A32D2D" },
                { name: "Mass larvicide + fogging — all RTs", impact: "75% risk cut", cost: "$6,000", badge: "🚨 Critical", bc: "#FCEBEB", bt: "#A32D2D" },
                { name: "Emergency BPJS enrollment — all uninsured", impact: "~12 people", cost: "$600", badge: "Urgent", bc: "#FCEBEB", bt: "#A32D2D" },
                { name: "Food + medicine distribution — low income", impact: "25 families", cost: "$2,500", badge: "Urgent", bc: "#FCEBEB", bt: "#A32D2D" },
                { name: "CHW deployment — all gap areas", impact: "RT 09,12,15", cost: "$1,900", badge: "High impact", bc: "#EAF3DE", bt: "#3B6D11" },
                { name: "Reserve — ongoing response", impact: "–", cost: "$31,000", badge: "Reserve", bc: "#f5f5f5", bt: "#888" },
              ],
              note: "CRITICAL — activate full emergency response. All community gaps must be covered by government resources immediately.",
            },
          };
          const alloc = allocationByTier[riskLevel];
          const usedPct = alloc.pct;
          return (
          <div>
            <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 6 }}>Budget allocation</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: 20 }}>
              Knapsack-optimized · {selectedCountry} · {riskLevel.toUpperCase()} risk week · gaps not covered by community
            </div>
            <div style={{ background: riskIdx >= 3 ? "#FCEBEB" : "#f5f5f5", borderRadius: 10, padding: "14px 18px", marginBottom: 20, border: riskIdx >= 3 ? "0.5px solid #F09595" : "none" }}>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 14, marginBottom: 8 }}>
                <span style={{ color: "#666" }}>Budget deployed this week</span>
                <span style={{ fontWeight: 700 }}>${alloc.used.toLocaleString()} / $50,000</span>
              </div>
              <div style={{ height: 10, background: "#e0e0e0", borderRadius: 5 }}>
                <div style={{ width: usedPct, height: "100%", background: riskIdx >= 2 ? "#E24B4A" : "#1D9E75", borderRadius: 5, transition: "width 0.5s" }} />
              </div>
              <div style={{ fontSize: 12, color: riskIdx >= 2 ? "#A32D2D" : "#3B6D11", marginTop: 6 }}>{alloc.note}</div>
            </div>
            <div style={{ background: "#fff", border: "0.5px solid #eee", borderRadius: 12, padding: "16px", marginBottom: 12 }}>
              {alloc.items.map((r, i, arr) => (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 0", borderBottom: i < arr.length - 1 ? "0.5px solid #eee" : "none" }}>
                  <span style={{ flex: 1, fontSize: 14 }}>{r.name}</span>
                  <span style={{ fontSize: 12, color: "#888", minWidth: 100, textAlign: "right" }}>{r.impact}</span>
                  <span style={{ fontSize: 15, fontWeight: 700, minWidth: 80, textAlign: "right" }}>{r.cost}</span>
                  <span style={{ fontSize: 11, padding: "3px 10px", borderRadius: 6, background: r.bc, color: r.bt, minWidth: 90, textAlign: "center", fontWeight: 500 }}>{r.badge}</span>
                </div>
              ))}
            </div>
          </div>
          );
        })()}

        {/* GOV GAPS */}
        {activePage === "gov-gaps" && (
          <div>
            <div style={{ fontSize: 20, fontWeight: 600, marginBottom: 4 }}>Coverage gaps</div>
            <div style={{ fontSize: 13, color: "#666", marginBottom: 16 }}>Needs that community resources cannot meet</div>
            <div style={{ background: "#FCEBEB", border: "0.5px solid #F09595", borderRadius: 8, padding: "10px 14px", fontSize: 13, color: "#A32D2D", marginBottom: 16 }}>
              ▲ RT 09, 12, 15 have zero community resource providers — highest priority for government deployment.
            </div>
            {[
              { priority: "elderly", label: "Unmatched · Elderly", name: "Mbah Sutarni — no volunteer match", detail: "Community has no available regular check-in volunteer for RT 09. Government volunteer program or CHW assignment needed." },
              { priority: "unemployed", label: "Unmatched · Low income", name: "Keluarga Eko — no BPJS, no food bank nearby", detail: "Food bank in RT 07 is too far. Not enrolled in PKH. BPJS PBI enrollment pending." },
            ].map((n, i) => {
              const pc = PRIORITY_COLORS[n.priority];
              return (
                <div key={i} style={{ borderLeft: `3px solid ${pc.border}`, background: "#fff", border: "0.5px solid #eee", borderLeftWidth: 3, borderLeftColor: pc.border, borderRadius: "0 8px 8px 0", padding: "10px 12px", marginBottom: 10 }}>
                  <span style={{ fontSize: 11, padding: "2px 8px", borderRadius: 6, background: pc.bg, color: pc.text, fontWeight: 500, display: "inline-block", marginBottom: 6 }}>{n.label}</span>
                  <div style={{ fontSize: 13, fontWeight: 500, marginBottom: 2 }}>{n.name}</div>
                  <div style={{ fontSize: 12, color: "#666" }}>{n.detail}</div>
                </div>
              );
            })}
            <div style={{ background: "#f5f5f5", borderRadius: 8, padding: "12px", marginTop: 8 }}>
              <div style={{ fontSize: 13, fontWeight: 500, marginBottom: 4 }}>Systematic gap: 3 RTs with no community resource providers</div>
              <div style={{ fontSize: 13, color: "#666" }}>Recommend: kader recruitment drive in RT 09, 12, 15. Estimated cost: $1,800/month for 3 part-time kaders.</div>
            </div>
          </div>
        )}

      </div>
    </div>
  );
}
