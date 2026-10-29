/* Panel ETL — JavaScript nativo, sin librerías. Las gráficas son SVG escrito a mano. */
"use strict";

const PALETA = ["#1f3a5f", "#2f6fb5", "#0f8f7e", "#c9821a", "#c2453c", "#6b5b95", "#2e8b57"];
const $ = (s) => document.querySelector(s);

const fmt = (n) => Number(n || 0).toLocaleString("es-MX");
const money = (n) => "$" + Number(n || 0).toLocaleString("es-MX",
  { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const esc = (s) => String(s ?? "").replace(/[&<>"]/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

async function api(ruta) {
  try {
    const r = await fetch(ruta, { cache: "no-store" });
    if (!r.ok) throw new Error(r.status);
    return await r.json();
  } catch (e) {
    console.warn("fallo", ruta, e);
    return null;
  }
}
const vacio = (msg) => `<div class="vacio">${esc(msg)}</div>`;

/* ============================================================ gráficas SVG */
function barrasH(datos, { alto = 26, color = null, formato = fmt } = {}) {
  if (!datos || !datos.length) return vacio("Sin datos");
  const max = Math.max(...datos.map((d) => d.valor), 1);
  const anchoEtq = 210, anchoVal = 78, ancho = 640;
  const h = datos.length * (alto + 9) + 8;
  let s = `<svg viewBox="0 0 ${ancho} ${h}" role="img">`;
  datos.forEach((d, i) => {
    const y = i * (alto + 9);
    const w = Math.max((d.valor / max) * (ancho - anchoEtq - anchoVal), 2);
    const c = color || PALETA[i % PALETA.length];
    s += `<text class="svg-etq" x="0" y="${y + alto / 2 + 4}">${esc(d.etiqueta)}</text>`;
    s += `<rect x="${anchoEtq}" y="${y}" width="${w}" height="${alto}" rx="4" fill="${c}"/>`;
    s += `<text class="svg-val" x="${anchoEtq + w + 9}" y="${y + alto / 2 + 4}">${formato(d.valor)}</text>`;
  });
  return s + "</svg>";
}

function dona(datos) {
  if (!datos || !datos.length) return vacio("Sin datos");
  const total = datos.reduce((a, d) => a + d.valor, 0) || 1;
  const cx = 120, cy = 120, R = 95, r = 58;
  let ang = -Math.PI / 2, s = `<svg viewBox="0 0 470 245" role="img">`;
  datos.forEach((d, i) => {
    const frac = d.valor / total, da = frac * 2 * Math.PI, fin = ang + da;
    const grande = da > Math.PI ? 1 : 0;
    const p = (rad, a) => [cx + rad * Math.cos(a), cy + rad * Math.sin(a)];
    const [x1, y1] = p(R, ang), [x2, y2] = p(R, fin);
    const [x3, y3] = p(r, fin), [x4, y4] = p(r, ang);
    s += `<path d="M${x1} ${y1} A${R} ${R} 0 ${grande} 1 ${x2} ${y2} L${x3} ${y3} A${r} ${r} 0 ${grande} 0 ${x4} ${y4} Z" fill="${d.color || PALETA[i % PALETA.length]}"/>`;
    ang = fin;
  });
  s += `<text x="${cx}" y="${cy - 2}" text-anchor="middle" style="font-size:25px;font-weight:700;fill:#16202e">${fmt(total)}</text>`;
  s += `<text x="${cx}" y="${cy + 18}" text-anchor="middle" class="svg-etq">registros</text>`;
  datos.forEach((d, i) => {
    const y = 48 + i * 32;
    s += `<rect x="265" y="${y - 11}" width="13" height="13" rx="3" fill="${d.color || PALETA[i % PALETA.length]}"/>`;
    s += `<text class="svg-val" x="286" y="${y}">${esc(d.etiqueta)}</text>`;
    s += `<text class="svg-etq" x="286" y="${y + 15}">${fmt(d.valor)} · ${(100 * d.valor / total).toFixed(2)}%</text>`;
  });
  return s + "</svg>";
}

function embudo(etapas) {
  if (!etapas || !etapas.length) return vacio("Sin datos");
  const max = Math.max(...etapas.map((e) => e.registros), 1);
  const ancho = 700, altoF = 40, hueco = 7;
  const h = etapas.length * (altoF + hueco);
  let s = `<svg viewBox="0 0 ${ancho} ${h}" role="img">`;
  etapas.forEach((e, i) => {
    const y = i * (altoF + hueco);
    const w = Math.max((e.registros / max) * 430, 6);
    const c = PALETA[i % PALETA.length];
    s += `<text class="svg-val" x="0" y="${y + 17}">Fase ${esc(e.fase)} · ${esc(e.nombre)}</text>`;
    s += `<text class="svg-etq" x="0" y="${y + 32}">${esc(e.detalle)}</text>`;
    s += `<rect x="252" y="${y + 4}" width="${w}" height="${altoF - 10}" rx="4" fill="${c}" opacity="${1 - i * 0.055}"/>`;
    s += `<text class="svg-val" x="${252 + w + 9}" y="${y + altoF / 2 + 3}">${fmt(e.registros)}</text>`;
  });
  return s + "</svg>";
}

function barrasAgrupadas(grupos, series) {
  if (!grupos.length) return vacio("Sin datos");
  const ancho = 660, alto = 270, mi = 62, md = 14, ms = 18, mb = 46;
  const aU = ancho - mi - md, aG = aU / grupos.length;
  const aB = (aG * 0.74) / series.length;
  const max = Math.max(...series.flatMap((s) => s.valores), 1);
  const esc_ = (v) => (v / max) * (alto - ms - mb);
  let s = `<svg viewBox="0 0 ${ancho} ${alto}" role="img">`;
  for (let i = 0; i <= 4; i++) {
    const y = ms + (i / 4) * (alto - ms - mb);
    s += `<line class="svg-eje" x1="${mi}" y1="${y}" x2="${ancho - md}" y2="${y}"/>`;
    s += `<text class="svg-etq" x="${mi - 7}" y="${y + 4}" text-anchor="end">${money(max * (1 - i / 4)).replace(/\.00$/, "")}</text>`;
  }
  grupos.forEach((g, gi) => {
    const x0 = mi + gi * aG + aG * 0.13;
    series.forEach((se, si) => {
      const v = se.valores[gi] || 0, h = esc_(v);
      s += `<rect x="${x0 + si * aB}" y="${alto - mb - h}" width="${aB - 3}" height="${Math.max(h, 1)}" rx="3" fill="${PALETA[si]}"/>`;
    });
    s += `<text class="svg-etq" x="${mi + gi * aG + aG / 2}" y="${alto - mb + 17}" text-anchor="middle">${esc(g)}</text>`;
  });
  series.forEach((se, si) => {
    s += `<rect x="${mi + si * 128}" y="${alto - 17}" width="11" height="11" rx="2" fill="${PALETA[si]}"/>`;
    s += `<text class="svg-etq" x="${mi + si * 128 + 17}" y="${alto - 8}">${esc(se.nombre)}</text>`;
  });
  return s + "</svg>";
}

function linea(puntos, { formato = money } = {}) {
  if (!puntos || puntos.length < 2) return vacio("Se necesitan al menos dos días de datos");
  const ancho = 700, alto = 240, mi = 72, md = 16, ms = 16, mb = 42;
  const max = Math.max(...puntos.map((p) => p.valor), 1);
  const px = (i) => mi + (i / (puntos.length - 1)) * (ancho - mi - md);
  const py = (v) => ms + (1 - v / max) * (alto - ms - mb);
  let s = `<svg viewBox="0 0 ${ancho} ${alto}" role="img">`;
  for (let i = 0; i <= 4; i++) {
    const y = ms + (i / 4) * (alto - ms - mb);
    s += `<line class="svg-eje" x1="${mi}" y1="${y}" x2="${ancho - md}" y2="${y}"/>`;
    s += `<text class="svg-etq" x="${mi - 7}" y="${y + 4}" text-anchor="end">${formato(max * (1 - i / 4)).replace(/\.00$/, "")}</text>`;
  }
  const d = puntos.map((p, i) => `${i ? "L" : "M"}${px(i)} ${py(p.valor)}`).join(" ");
  const area = `${d} L${px(puntos.length - 1)} ${alto - mb} L${px(0)} ${alto - mb} Z`;
  s += `<path d="${area}" fill="${PALETA[1]}" opacity=".13"/>`;
  s += `<path d="${d}" fill="none" stroke="${PALETA[1]}" stroke-width="2.2"/>`;
  puntos.forEach((p, i) => { s += `<circle cx="${px(i)}" cy="${py(p.valor)}" r="2.6" fill="${PALETA[1]}"/>`; });
  const paso = Math.max(1, Math.ceil(puntos.length / 9));
  puntos.forEach((p, i) => {
    if (i % paso === 0) s += `<text class="svg-etq" x="${px(i)}" y="${alto - mb + 17}" text-anchor="middle">${esc(p.etiqueta)}</text>`;
  });
  return s + "</svg>";
}

/* ================================================================ vistas */
async function vistaResumen() {
  const r = await api("/api/resumen");
  if (!r) { $("#kpis").innerHTML = vacio("No se pudo leer el resumen."); return; }

  $("#sello-bd").textContent = r.bd_existe ? `base cargada · ${fmt(r.bd_kb)} KB` : "base no generada";
  $("#sello-bd").className = "sello" + (r.bd_existe ? "" : " malo");
  $("#actualizado").textContent = r.bd_existe ? `Actualizado ${r.actualizado}` : "";

  $("#kpis").innerHTML = [
    ["Registros procesados", fmt(r.entrada), "entrada de la Fase 5", ""],
    ["Validados", fmt(r.limpias), `${r.tasa_aceptacion}% de aceptación`, "ok"],
    ["En cuarentena", fmt(r.cuarentena), "aislados con su motivo", "alerta"],
    ["Monto facturable", money(r.monto_facturable), `ticket medio ${money(r.ticket_promedio)}`, "dinero"],
    ["Pruebas de estrés", fmt(r.pruebas_estres), `${r.crashes} excepciones no controladas`, r.crashes === 0 ? "ok" : "alerta"],
  ].map(([e, v, p, c]) => `<div class="kpi ${c}"><div class="etq">${e}</div><div class="val">${v}</div><div class="pie">${p}</div></div>`).join("");

  $("#dona-bifurcacion").innerHTML = dona([
    { etiqueta: "transacciones_limpias", valor: r.limpias, color: PALETA[2] },
    { etiqueta: "cuarentena_errores", valor: r.cuarentena, color: PALETA[3] },
  ]);

  const q = await api("/api/cuarentena?limite=1");
  $("#barras-cuarentena").innerHTML = q && q.resumen.length
    ? barrasH(q.resumen.map((x) => ({ etiqueta: x.causa, valor: x.registros })))
    : vacio("Sin cuarentena registrada");

  const fases = await api("/api/fases");
  $("#pasos-resumen").innerHTML = (fases || []).map((f) =>
    `<div class="paso"><span class="n">${esc(f.fase)}</span><strong>${esc(f.nombre)}</strong><span>${esc(f.detalle)}</span></div>`).join("");
}

async function vistaFlujo() {
  const f = await api("/api/fases");
  $("#embudo").innerHTML = embudo(f);
  $("#tabla-fases").innerHTML = !f ? vacio("Sin datos") : `<div class="tabla-scroll"><table>
    <thead><tr><th>Fase</th><th>Nombre</th><th>Qué hace</th><th class="num">Registros</th></tr></thead>
    <tbody>${f.map((x) => `<tr><td><strong>${esc(x.fase)}</strong></td><td>${esc(x.nombre)}</td>
      <td>${esc(x.detalle)}</td><td class="num">${fmt(x.registros)}</td></tr>`).join("")}</tbody></table></div>`;
}

async function vistaEnganosos() {
  const d = await api("/api/enganosos");
  if (!d || !d.length) { $("#tarjetas-enganosos").innerHTML = vacio("Corre el análisis forense para ver los hallazgos."); return; }
  $("#tarjetas-enganosos").innerHTML = d.map((x) => `
    <div class="eng ${x.afectados === 0 ? "cero" : ""}">
      <div class="eng-cab">
        <span class="eng-cod">${esc(x.codigo)}</span>
        <span class="eng-tit">${esc(x.titulo)}</span>
        <span class="eng-n">${fmt(x.afectados)} de ${fmt(x.total)} · ${x.pct}%</span>
      </div>
      <div class="eng-fila"><span class="eng-etq">Por qué engaña</span><span>${esc(x.por_que)}</span></div>
      <div class="eng-fila"><span class="eng-etq">Evidencia</span><span>${esc(x.evidencia)}</span></div>
      <div class="eng-fila"><span class="eng-etq">Acción</span><span>${esc(x.accion)}</span></div>
    </div>`).join("");
}

let causaActiva = null;
async function vistaCuarentena() {
  const d = await api(`/api/cuarentena?limite=250${causaActiva ? "&causa=" + encodeURIComponent(causaActiva) : ""}`);
  if (!d) { $("#tabla-cuarentena").innerHTML = vacio("Sin datos"); return; }

  $("#filtros-cuarentena").innerHTML =
    `<button class="chip ${!causaActiva ? "activo" : ""}" data-causa="">Todas</button>` +
    d.resumen.map((x) => `<button class="chip ${causaActiva === x.causa ? "activo" : ""}" data-causa="${esc(x.causa)}">${esc(x.causa)} · ${fmt(x.registros)}</button>`).join("");
  $("#filtros-cuarentena").querySelectorAll(".chip").forEach((b) =>
    b.onclick = () => { causaActiva = b.dataset.causa || null; vistaCuarentena(); });

  $("#tabla-cuarentena").innerHTML = !d.registros.length ? vacio("Sin registros para este filtro") :
    `<div class="tabla-scroll"><table>
      <thead><tr><th>Registro</th><th>Canal</th><th>Producto</th><th class="num">Monto</th><th>Fecha</th><th>Causa</th><th>Motivo exacto</th></tr></thead>
      <tbody>${d.registros.map((r) => `<tr>
        <td class="mono">${esc(r.id_registro)}</td><td>${esc(r.canal)}</td>
        <td class="mono">${esc(r.id_producto)}</td>
        <td class="num">${r.monto_mxn ? money(r.monto_mxn) : "—"}</td>
        <td>${esc(r.fecha || "—")}</td>
        <td><span class="pil alerta">${esc(r.causa_principal)}</span></td>
        <td class="tenue">${esc(r.motivo_rechazo_cuarentena)}</td></tr>`).join("")}</tbody></table></div>
      <p class="tenue">Mostrando ${d.registros.length} registros.</p>`;
}

async function vistaNegocio() {
  const s = await api("/api/segmentos");
  if (s && s.cruce.length) {
    const canales = [...new Set(s.cruce.map((x) => x.canal))];
    const segs = ["BAJO", "MEDIO", "ALTO"];
    $("#barras-segmento").innerHTML = barrasAgrupadas(segs, canales.map((c) => ({
      nombre: c, valores: segs.map((g) => {
        const f = s.cruce.find((x) => x.canal === c && x.segmento === g);
        return f ? Number(f.facturado) : 0;
      }),
    })));
    $("#tabla-segmentos").innerHTML = `<table>
      <thead><tr><th>Segmento</th><th class="num">Registros</th><th class="num">Mínimo</th><th class="num">Máximo</th><th class="num">Promedio</th></tr></thead>
      <tbody>${s.rangos.map((r) => `<tr><td><strong>${esc(r.segmento_monto)}</strong></td>
        <td class="num">${fmt(r.n)}</td><td class="num">${money(r.minimo)}</td>
        <td class="num">${money(r.maximo)}</td><td class="num">${money(r.promedio)}</td></tr>`).join("")}</tbody></table>`;
  } else {
    $("#barras-segmento").innerHTML = vacio("Sin datos");
    $("#tabla-segmentos").innerHTML = vacio("Sin datos");
  }

  const serie = await api("/api/serie");
  $("#linea-serie").innerHTML = linea((serie || []).map((p) => ({
    etiqueta: String(p.fecha).slice(5), valor: Number(p.facturado),
  })));

  const prods = await api("/api/productos");
  $("#tabla-productos").innerHTML = !prods || !prods.length ? vacio("Sin datos") :
    `<div class="tabla-scroll"><table>
      <thead><tr><th>Producto</th><th>Nombre</th><th>Categoría</th><th class="num">Pedidos</th><th class="num">Facturado</th></tr></thead>
      <tbody>${prods.map((p) => `<tr><td class="mono">${esc(p.id_producto)}</td>
        <td>${esc(p.nombre)}</td><td>${esc(p.categoria)}</td>
        <td class="num">${fmt(p.pedidos)}</td><td class="num">${money(p.facturado)}</td></tr>`).join("")}</tbody></table></div>`;
}

async function vistaEstres() {
  const d = await api("/api/estres");
  if (!d || !d.length) { $("#tabla-estres").innerHTML = vacio("Corre la Fase 6 para generar las pruebas."); return; }
  const crash = d.filter((x) => x.resultado === "CRASH").length;
  $("#tabla-estres").innerHTML = `
    <p class="sub">${d.length} casos · ${d.filter((x) => x.resultado === "RECHAZADO").length} rechazados ·
      ${d.filter((x) => x.resultado === "ACEPTADO").length} aceptado (el control válido) ·
      <strong style="color:${crash ? "#c2453c" : "#2e8b57"}">${crash} excepciones no controladas</strong></p>
    <div class="tabla-scroll"><table>
      <thead><tr><th>Caso inyectado</th><th>Resultado</th><th>Mensaje del validador</th></tr></thead>
      <tbody>${d.map((r) => `<tr><td>${esc(r.caso)}</td>
        <td><span class="pil ${r.resultado === "CRASH" ? "malo" : r.resultado === "ACEPTADO" ? "neutro" : "ok"}">${esc(r.resultado)}</span></td>
        <td class="tenue">${esc(r.detalle || "—")}</td></tr>`).join("")}</tbody></table></div>`;
}

async function vistaQA() {
  const d = await api("/api/qa");
  if (!d || !d.resumen.length) {
    $("#tabla-qa").innerHTML = vacio("Corre: python qa/suite_qa.py --completo");
    $("#grafico-escala").innerHTML = "";
    return;
  }
  const fallos = d.resumen.reduce((a, r) => a + Number(r.fallos || 0), 0);
  const checks = d.resumen.reduce((a, r) => a + Number(r.checks || 0), 0);
  $("#tabla-qa").innerHTML = `
    <p class="sub"><strong>${fmt(checks)} verificaciones</strong> en ${d.resumen.length} escenarios ·
      <strong style="color:${fallos ? "#c2453c" : "#2e8b57"}">${fallos} fallos</strong></p>
    <div class="tabla-scroll"><table>
      <thead><tr><th>Escenario</th><th class="num">Checks</th><th class="num">Fallos</th>
        <th class="num">Entrada</th><th class="num">Limpias</th><th class="num">Cuarentena</th>
        <th class="num">Seg.</th><th>Estado</th></tr></thead>
      <tbody>${d.resumen.map((r) => `<tr><td class="mono">${esc(r.escenario)}</td>
        <td class="num">${fmt(r.checks)}</td><td class="num">${fmt(r.fallos)}</td>
        <td class="num">${fmt(r.filas_unificado)}</td><td class="num">${fmt(r.filas_limpias)}</td>
        <td class="num">${fmt(r.filas_cuarentena)}</td><td class="num">${r.total_s || "—"}</td>
        <td><span class="pil ${String(r.ok).toLowerCase() === "true" ? "ok" : "malo"}">${String(r.ok).toLowerCase() === "true" ? "OK" : "REVISAR"}</span></td></tr>`).join("")}</tbody></table></div>`;

  const vol = d.resumen.filter((r) => r.filas_unificado && r.total_s)
    .map((r) => ({ etiqueta: `${fmt(r.filas_unificado)} filas`, valor: Number(r.total_s) }))
    .sort((a, b) => a.valor - b.valor);
  $("#grafico-escala").innerHTML = barrasH(vol, { color: PALETA[1], formato: (v) => v.toFixed(1) + " s" });
}

/* =============================================================== ejecutar */
let sondeo = null;
function pintarEstado(e) {
  const btn = $("#btn-ejecutar");
  btn.disabled = e.activo;
  btn.textContent = e.activo ? "Ejecutando…" : "Ejecutar pipeline";
  $("#barra-progreso").style.width = `${(e.paso / (e.total_pasos || 6)) * 100}%`;
  $("#estado-ejecucion").textContent = e.activo
    ? `${e.fase} — paso ${e.paso} de ${e.total_pasos} (${e.segundos}s)`
    : e.ok === true ? `Completado en ${e.segundos}s.`
      : e.ok === false ? "Se detuvo por un error. Revisa la consola."
        : "Listo.";
  const c = $("#consola");
  const abajo = c.scrollHeight - c.scrollTop - c.clientHeight < 60;
  if (e.lineas && e.lineas.length) c.textContent = e.lineas.join("\n");
  if (abajo) c.scrollTop = c.scrollHeight;
}

async function sondear() {
  const e = await api("/api/estado");
  if (!e) return;
  pintarEstado(e);
  if (!e.activo && sondeo) {
    clearInterval(sondeo); sondeo = null;
    await vistaResumen();                       // refrescar KPIs con la base nueva
  }
}

$("#btn-ejecutar").onclick = async () => {
  $("#consola").textContent = "Iniciando…";
  const r = await fetch("/api/ejecutar", { method: "POST" }).then((x) => x.json()).catch(() => null);
  if (!r || !r.ok) { $("#estado-ejecucion").textContent = r ? r.mensaje : "No se pudo iniciar."; return; }
  if (sondeo) clearInterval(sondeo);
  sondeo = setInterval(sondear, 900);
  sondear();
};


/* ===================================================== motor universal */
let corrida = null, sondeoU = null, elegidos = [];
const zona = $("#zona"), inp = $("#archivo");
zona.onclick = () => inp.click();
["dragenter", "dragover"].forEach((e) => zona.addEventListener(e, (ev) => { ev.preventDefault(); zona.classList.add("sobre"); }));
["dragleave", "drop"].forEach((e) => zona.addEventListener(e, (ev) => { ev.preventDefault(); zona.classList.remove("sobre"); }));
zona.addEventListener("drop", (ev) => elegir([...ev.dataTransfer.files]));
inp.onchange = () => elegir([...inp.files]);

function elegir(fs) {
  elegidos = fs.filter((f) => f.size > 0);
  $("#lista-archivos").innerHTML = elegidos.length
    ? elegidos.map((f) => `• ${esc(f.name)} (${(f.size / 1048576).toFixed(2)} MB)`).join("<br>")
    : "Ningún archivo válido (vacío).";
  $("#btn-universal").disabled = !elegidos.length;
  $("#estado-universal").textContent = elegidos.length ? `${elegidos.length} archivo(s) listos.` : "Elige uno o más archivos.";
}

$("#btn-universal").onclick = async () => {
  const btn = $("#btn-universal"); btn.disabled = true; corrida = null;
  $("#resultado-u").innerHTML = "";
  try {
    for (const [i, f] of elegidos.entries()) {
      $("#estado-universal").textContent = `Subiendo ${i + 1}/${elegidos.length}: ${f.name}…`;
      const r = await fetch(`/api/universal/subir${corrida ? "?id=" + corrida : ""}`, {
        method: "POST", body: f, headers: { "X-Filename": encodeURIComponent(f.name) } });
      const d = await r.json();
      if (!d.ok) throw new Error(d.mensaje || "no se pudo subir");
      corrida = d.id;
    }
    $("#estado-universal").textContent = "Procesando…";
    const r = await fetch(`/api/universal/ejecutar?id=${corrida}`, { method: "POST" }).then((x) => x.json());
    if (!r.ok) throw new Error(r.mensaje);
    sondeoU = setInterval(sondearU, 800);
  } catch (e) {
    $("#estado-universal").textContent = "Error: " + e.message; btn.disabled = false;
  }
};

async function sondearU() {
  const e = await api(`/api/universal/estado?id=${corrida}`);
  if (!e) return;
  const c = $("#consola-u");
  c.textContent = (e.lineas || []).join("\n"); c.scrollTop = c.scrollHeight;
  $("#estado-universal").textContent = e.activo ? `Procesando… ${e.segundos}s` :
    e.ok ? `Terminado en ${e.segundos}s.` : "Se detuvo de forma controlada (ver consola).";
  if (!e.activo) {
    clearInterval(sondeoU); sondeoU = null; $("#btn-universal").disabled = false;
    pintarReporte(await api(`/api/universal/reporte?id=${corrida}`));
  }
}

function tabla(filas, cols, titulos) {
  if (!filas || !filas.length) return vacio("Sin registros");
  return `<div class="tabla-scroll"><table><thead><tr>${(titulos || cols).map((t) => `<th>${esc(t)}</th>`).join("")}</tr></thead>
    <tbody>${filas.map((f) => `<tr>${cols.map((c) => `<td>${esc(f[c] ?? "")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

function pintarReporte(r) {
  if (!r) { $("#resultado-u").innerHTML = vacio("Sin reporte."); return; }
  if (!r.ok) { $("#resultado-u").innerHTML = `<div class="aviso"><strong>El motor se detuvo sin corromper nada.</strong> ${esc(r.error || "")}</div>`; return; }
  const t = r.totales;
  let h = `<div class="kpis">
    <div class="kpi"><div class="etq">Registros</div><div class="val">${fmt(t.entrada)}</div><div class="pie">${Object.keys(r.fuentes).length} fuente(s) · ${r.segundos}s</div></div>
    <div class="kpi ok"><div class="etq">Válidos</div><div class="val">${fmt(t.limpias)}</div><div class="pie">${t.tasa_aceptacion}% de aceptación</div></div>
    <div class="kpi alerta"><div class="etq">Cuarentena</div><div class="val">${fmt(t.cuarentena)}</div><div class="pie">con motivo por registro</div></div>
    <div class="kpi"><div class="etq">Rollback</div><div class="val">${r.bd && r.bd._rollback_verificado ? "OK" : "—"}</div><div class="pie">verificado en SQLite</div></div></div>`;
  h += `<div class="tarjeta descargas"><h2>Descargas</h2>${(r.archivos || []).map((f) =>
    `<a href="/api/universal/descargar?id=${corrida}&f=${encodeURIComponent(f)}">${esc(f)}</a>`).join("")}</div>`;
  if (r.relaciones && r.relaciones.length) h += `<div class="tarjeta"><h2>Relaciones detectadas (catálogo maestro)</h2>
    ${tabla(r.relaciones, ["hija", "columna", "maestro", "llave", "huerfanos"], ["Tabla", "Columna", "Catálogo", "Llave", "Huérfanos"])}</div>`;
  for (const [nombre, f] of Object.entries(r.fuentes)) {
    if (f.vacio) { h += `<div class="tarjeta"><h2>${esc(nombre)}</h2>${vacio("Sin filas útiles.")}</div>`; continue; }
    const f6 = f.fase6 || {}, f34 = f.fase34 || {}, f5 = f.fase5 || {};
    h += `<div class="tarjeta"><div class="fuente-cab"><h2>${esc(nombre)}</h2>
      <span class="tenue">${fmt(f6.validas)} válidos · ${fmt(f6.cuarentena)} cuarentena · completitud ${f.perfil.completitud_pct}%</span></div>
      <h3 class="sub">Fase 1 · Perfil y tipos inferidos</h3>
      ${tabla(f.perfil.columnas, ["columna", "tipo", "nulos", "pct_nulos", "unicos", "ejemplo"], ["Columna", "Tipo", "Nulos", "% nulos", "Únicos", "Ejemplo"])}
      <h3 class="sub">Fase 2 · Faltantes (MCAR/MAR) e imputación</h3>
      ${tabla(f.faltantes, ["columna", "faltantes", "pct", "clasificacion", "p_valor", "estrategia"], ["Columna", "Faltan", "%", "Tipo", "p", "Estrategia"])}
      <h3 class="sub">Fase 3 · Outliers IQR (capping, sin borrar filas)</h3>
      ${tabla(f34.iqr, ["columna", "q1", "q3", "limite_inf", "limite_sup", "recortados"], ["Columna", "Q1", "Q3", "Lím. inf", "Lím. sup", "Recortados"])}
      <h3 class="sub">Fase 4 · Fechas a ISO</h3>
      ${tabla(f34.fechas, ["columna", "convertidas", "no_convertibles", "formatos"], ["Columna", "Convertidas", "No convertibles", "Formatos"])}
      ${f34.aritmetica ? `<p class="sub">Aritmética ${esc(f34.aritmetica.total)} = ${esc(f34.aritmetica.cantidad)} × ${esc(f34.aritmetica.precio)}: ${fmt(f34.aritmetica.inconsistentes)} inconsistentes, ${fmt(f34.aritmetica.patron_bot)} con patrón de bot.</p>` : ""}
      <h3 class="sub">Fase 5 · Discretización</h3>
      <p>${f5.discretizacion ? `<strong>${esc(f5.discretizacion.columna)}</strong> · cortes ${esc(f5.discretizacion.cortes.join(" | "))} · ${esc(JSON.stringify(f5.discretizacion.reparto))}` : "—"}</p>
      <h3 class="sub">Fase 6 · Causas de cuarentena</h3>
      ${barrasH(Object.entries(f6.causas || {}).map(([k, v]) => ({ etiqueta: k, valor: v })))}
      ${tabla(f.muestra_cuarentena, ["causa_principal", "motivo_rechazo_cuarentena"], ["Causa", "Motivo exacto"])}
    </div>`;
  }
  $("#resultado-u").innerHTML = h;
}

/* ============================================================ navegación */
const VISTAS = {
  resumen: vistaResumen, flujo: vistaFlujo, enganosos: vistaEnganosos,
  cuarentena: vistaCuarentena, negocio: vistaNegocio, estres: vistaEstres,
  qa: vistaQA, ejecutar: sondear, universal: () => {},
};
const cargadas = new Set();

function ir(nombre) {
  document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("activa", t.dataset.vista === nombre));
  document.querySelectorAll(".vista").forEach((v) => v.classList.toggle("activa", v.id === "v-" + nombre));
  if (!cargadas.has(nombre)) { cargadas.add(nombre); (VISTAS[nombre] || (() => {}))(); }
  location.hash = nombre;
}

const nombreValido = () => {
  const n = (location.hash || "#resumen").slice(1);
  return n in VISTAS ? n : "resumen";
};

document.querySelectorAll(".tab").forEach((t) => t.onclick = () => ir(t.dataset.vista));
// Permite enlaces directos y el botón Atrás del navegador.
window.addEventListener("hashchange", () => ir(nombreValido()));
ir(nombreValido());
