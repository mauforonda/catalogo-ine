const source = "https://raw.githubusercontent.com/mauforonda/catalogo-ine/refs/heads/master/catalogo.csv";
const input = document.querySelector("input");
const list = document.querySelector("#datasets");
const sentinel = document.querySelector("#sentinel");
const catalogue = document.querySelector("#catalogue");
const searchBox = document.querySelector("#search");
const clear = document.querySelector("button");
let rows = [], filtered = [], shown = 0;
const timestamp = value => Number.isNaN(Date.parse(value)) ? 0 : Date.parse(value);

const icons = {
  file: '<path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z"/><path d="M14 2v5a1 1 0 0 0 1 1h5"/>',
  text: '<path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z"/><path d="M14 2v5a1 1 0 0 0 1 1h5M10 9H8M16 13H8M16 17H8"/>',
  sheet: '<path d="M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z"/><path d="M14 2v5a1 1 0 0 0 1 1h5M8 13h2M14 13h2M8 17h2M14 17h2"/>',
  archive: '<rect width="20" height="5" x="2" y="3" rx="1"/><path d="M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8m-10 4h4"/>',
};

const icon = type => {
  const name = type === "pdf" || type === "word" ? "text" : type === "spreadsheet" || type === "excel" ? "sheet" : type === "zip" || type === "rar" ? "archive" : "file";
  return `<svg class="format" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><title>${type}</title>${icons[name]}</svg>`;
};

const csv = text => {
  const data = [[]], cell = { value: "" };
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const character = text[i];
    if (character === '"' && quoted && text[i + 1] === '"') cell.value += text[++i];
    else if (character === '"') quoted = !quoted;
    else if (character === "," && !quoted) { data.at(-1).push(cell.value); cell.value = ""; }
    else if ((character === "\n" || character === "\r") && !quoted) {
      if (character === "\r" && text[i + 1] === "\n") i++;
      data.at(-1).push(cell.value); cell.value = ""; data.push([]);
    } else cell.value += character;
  }
  data.at(-1).push(cell.value);
  const [headers, ...values] = data;
  return values.filter(value => value.length > 1).map(value => Object.fromEntries(headers.map((header, i) => [header, value[i]])));
};

const render = () => {
  const batch = filtered.slice(shown, shown + 40);
  shown += batch.length;
  batch.forEach(row => {
    const dataset = document.createElement("div");
    dataset.className = "dataset";
    const modified = timestamp(row.modificado);
    const cutoff = new Date();
    cutoff.setMonth(cutoff.getMonth() - 11);
    const date = modified && new Intl.DateTimeFormat("es-BO", {
      day: "numeric", month: "long", ...(modified < cutoff ? { year: "numeric" } : {}), timeZone: "America/La_Paz",
    }).format(modified);
    const available = row.disponible?.toLowerCase() === "true";
    dataset.innerHTML = `<a><div class="top"><span class="name"></span><span class="status">${icon(row.tipo)}<span class="availability${available ? " available" : ""}" title="${available ? "Disponible" : "No disponible"}"></span></span></div><div class="page"></div><div class="meta"><span></span><span></span></div></a>`;
    dataset.querySelector("a").href = row.link;
    dataset.querySelector(".name").textContent = row.nombre;
    dataset.querySelector(".page").textContent = row.pagina;
    dataset.querySelector(".meta span").textContent = date || "";
    dataset.querySelector(".meta span + span").textContent = row.kb ? `${row.kb} KB` : "";
    list.append(dataset);
  });
};

const search = () => {
  const query = input.value.toLocaleLowerCase();
  filtered = rows.filter(row => [row.nombre, row.pagina].some(value => value.toLocaleLowerCase().includes(query)));
  shown = 0;
  list.replaceChildren();
  catalogue.scrollTop = 0;
  searchBox.classList.toggle("has-value", Boolean(input.value));
  render();
};

new IntersectionObserver(entries => entries[0].isIntersecting && render(), { root: catalogue, rootMargin: "0px 0px 200px" }).observe(sentinel);
input.addEventListener("input", search);
clear.addEventListener("click", () => { input.value = ""; input.focus(); search(); });

fetch(source).then(response => response.text()).then(text => {
  rows = csv(text).sort((a, b) => timestamp(b.modificado) - timestamp(a.modificado) || a.link.localeCompare(b.link));
  search();
});
