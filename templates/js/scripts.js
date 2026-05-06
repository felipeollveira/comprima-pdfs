/* =========================================================
   PDF Optimizer — Front-end (lote de até 3 PDFs, nível global)
   ========================================================= */

const pdfInput = document.getElementById('pdfInput');
const dropZone = document.getElementById('dropZone');
const dropText = document.getElementById('dropText');
const btnDownload = document.getElementById('btnDownload');
const btnClear = document.getElementById('btnClear');
const fileQueueEl = document.getElementById('fileQueue');

let selectedLevel = 3;        // nível global (1..6)
let selectedFiles = [];       // lista de File objects (máx 3)
let originalSizeMB = 0;
let isDownloading = false;
let currentEventSource = null;
let currentXHR = null;
let currentTaskId = null;
let extraCompressPages = new Set(); // páginas marcadas para compressão extra
let pdfAnalysisCache = new Map();   // cache de análise dos PDFs (pages, avgKbPerPage)

const MAX_FILES = 3;

/* =========================================================
   BALÃO CENTRAL (Download concluído) - showCenterToast
   ========================================================= */
(function () {
  const container = document.getElementById("toastContainer");
  if (!container) return;

  let isOpen = false;
  let onCloseCb = null;

  function closeToast() {
    if (!isOpen) return;
    isOpen = false;
    container.classList.remove("show");
    container.innerHTML = "";
    document.body.style.overflow = "";
    if (typeof onCloseCb === "function") {
      const fn = onCloseCb; onCloseCb = null;
      try { fn(); } catch (e) {}
    }
  }

  function showCenterToast({ title, message, type = "success", closeText = "Fechar", onClose = null }) {
    onCloseCb = onClose || null;
    container.innerHTML = `
      <div class="toast-backdrop" data-toast-close="1"></div>
      <div class="toast-box ${type}" role="dialog" aria-modal="true" aria-label="${title || "Aviso"}">
        <div class="toast-row">
          <div class="toast-icon">${type === "success" ? "✅" : type === "error" ? "❌" : "ℹ️"}</div>
          <div class="toast-content">
            <div class="toast-title">${title || "Aviso"}</div>
            <div class="toast-msg">${message || ""}</div>
          </div>
        </div>
        <div class="toast-actions">
          <button type="button" class="toast-btn toast-btn-close" id="toastCloseBtn">${closeText}</button>
        </div>
      </div>`;
    container.classList.add("show");
    isOpen = true;
    document.body.style.overflow = "hidden";

    const backdrop = container.querySelector("[data-toast-close='1']");
    if (backdrop) backdrop.addEventListener("click", closeToast);
    const btn = container.querySelector("#toastCloseBtn");
    if (btn) btn.addEventListener("click", closeToast);
    const onKeyDown = (e) => { if (e.key === "Escape") { closeToast(); document.removeEventListener("keydown", onKeyDown); } };
    document.addEventListener("keydown", onKeyDown);
  }

  window.showCenterToast = showCenterToast;
  window.closeCenterToast = closeToast;
})();

/* =========================================================
   SELEÇÃO DO NÍVEL GLOBAL
   ========================================================= */
document.querySelectorAll('.level-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.level-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selectedLevel = parseInt(btn.dataset.level, 10);
    atualizarEstimativa();
  });
});

/* =========================================================
   DROP ZONE + FILA DE ARQUIVOS
   ========================================================= */
dropZone.onclick = () => pdfInput.click();
dropZone.ondragover = (e) => { e.preventDefault(); dropZone.classList.add('drag-over'); dropText.innerText = "Solte para carregar"; };
dropZone.ondragleave = () => { dropZone.classList.remove('drag-over'); dropText.innerText = "Arraste PDFs aqui ou clique"; };
dropZone.ondrop = (e) => {
  e.preventDefault();
  dropZone.classList.remove('drag-over');
  adicionarArquivos(e.dataTransfer.files);
};

pdfInput.onchange = (e) => adicionarArquivos(e.target.files);

function adicionarArquivos(fileList) {
  for (const f of fileList) {
    if (f.type !== "application/pdf") continue;
    if (selectedFiles.length >= MAX_FILES) {
      alert(`Máximo de ${MAX_FILES} PDFs por lote.`);
      break;
    }
    if (selectedFiles.some(sf => sf.name === f.name && sf.size === f.size)) continue;
    selectedFiles.push(f);
  }
  atualizarFila();
  atualizarEstimativa();

  if (selectedFiles.length > 0) {
    const dt = new DataTransfer();
    selectedFiles.forEach(f => dt.items.add(f));
    document.getElementById('hiddenFile').files = dt.files;
    dropText.innerText = `${selectedFiles.length} PDF(s) selecionado(s)`;
    btnDownload.disabled = false;
    btnDownload.textContent = "OTIMIZAR AGORA";
    // Gerar thumbnails para o primeiro PDF (single file)
    if (selectedFiles.length === 1) {
      renderPageThumbnails(selectedFiles[0]);
    } else {
      hidePagePanel();
    }
  }
}

function removerArquivo(idx) {
  const file = selectedFiles[idx];
  if (file) {
    const cacheKey = `${file.name}_${file.size}`;
    pdfAnalysisCache.delete(cacheKey);
  }
  selectedFiles.splice(idx, 1);
  atualizarFila();
  atualizarEstimativa();
  if (selectedFiles.length === 0) {
    limparTudo();
  } else {
    const dt = new DataTransfer();
    selectedFiles.forEach(f => dt.items.add(f));
    document.getElementById('hiddenFile').files = dt.files;
    dropText.innerText = `${selectedFiles.length} PDF(s) selecionado(s)`;
  }
}

function atualizarFila() {
  if (!fileQueueEl) return;
  if (selectedFiles.length === 0) { fileQueueEl.innerHTML = ''; return; }

  fileQueueEl.innerHTML = selectedFiles.map((f, i) => {
    const sizeMB = (f.size / (1024 * 1024)).toFixed(2);
    return `<div class="d-flex align-items-center justify-content-between bg-light rounded px-3 py-2 mb-1">
      <span class="small text-truncate" style="max-width:auto; z-index: 1;" title="${f.name}">${f.name} <span class="text-muted">(${sizeMB} MB)</span></span>
      <button type="button" class="btn btn-sm btn-outline-danger py-0 px-2" onclick="removerArquivo(${i})" title="Remover">&times;</button>
    </div>`;
  }).join('');
}

/* =========================================================
   ESTIMATIVA INTELIGENTE
   ========================================================= */

// Analisa PDF para extrair número de páginas (usa cache)
async function analisarPDF(file) {
  const cacheKey = `${file.name}_${file.size}`;
  if (pdfAnalysisCache.has(cacheKey)) {
    return pdfAnalysisCache.get(cacheKey);
  }

  try {
    const arrayBuffer = await file.arrayBuffer();
    const pdfjsLib = window['pdfjs-dist/build/pdf'] || window.pdfjsLib;
    pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';
    const pdf = await pdfjsLib.getDocument({ data: arrayBuffer }).promise;

    const sizeMB = file.size / (1024 * 1024);
    const sizeKB = file.size / 1024;
    const pages = pdf.numPages;
    const avgKbPerPage = pages > 0 ? sizeKB / pages : sizeKB;

    const result = { pages, avgKbPerPage, sizeMB };
    pdfAnalysisCache.set(cacheKey, result);
    return result;
  } catch (err) {
    console.warn('Erro ao analisar PDF:', err);
    const sizeMB = file.size / (1024 * 1024);
    return { pages: 1, avgKbPerPage: file.size / 1024, sizeMB };
  }
}

// Calcula multiplicador inteligente baseado no conteúdo do PDF
function calcularMultiplicador(level, avgKbPerPage, totalPages) {
  // Multiplicadores base por nível
  // Nível 1 (Padrão): mantém alta fidelidade
  // Nível 2 (HQ): alta qualidade
  // Nível 3 (150 DPI): padrão
  // Nível 4 (72 DPI): agressivo
  // Nível 5 (50 DPI): muito agressivo
  // Nível 6 (OCR+Dividir): especial
  const baseMultMap = { 1: 0.95, 2: 0.80, 3: 0.55, 4: 0.25, 5: 0.12, 6: 0.08 };
  let baseMult = baseMultMap[level] || 0.55;

  // Ajuste baseado no tamanho médio por página (KB/página)
  // PDFs escaneados (alto KB/página) comprimem muito melhor
  // PDFs nativos/texto (baixo KB/página) têm pouca margem
  if (avgKbPerPage > 500) {
    // Páginas grandes (scans/imagens pesadas): compressão funciona muito bem
    baseMult *= 0.75;
  } else if (avgKbPerPage > 200) {
    // Páginas médias: compressão funciona bem
    baseMult *= 0.90;
  } else if (avgKbPerPage > 100) {
    // Páginas leves: compressão moderada
    baseMult *= 1.05;
  } else if (avgKbPerPage > 50) {
    // Páginas muito leves (texto com poucas imagens)
    baseMult *= 1.15;
  } else {
    // PDF quase puro texto: pouca compressão possível
    baseMult *= 1.30;
  }

  // Ajuste baseado no número de páginas
  // Documentos muito longos tendem a ter overhead otimizado
  if (totalPages > 50) {
    baseMult *= 0.95;
  } else if (totalPages > 20) {
    baseMult *= 0.98;
  }

  // Garantir limites razoáveis
  return Math.min(0.98, Math.max(0.05, baseMult));
}

// Atualiza estimativa de forma assíncrona
async function atualizarEstimativaAsync() {
  originalSizeMB = selectedFiles.reduce((sum, f) => sum + f.size / (1024 * 1024), 0);
  document.getElementById('origSize').innerText = originalSizeMB.toFixed(2);

  if (selectedFiles.length === 0) {
    document.getElementById('estSize').innerText = "0.00";
    document.getElementById('reduction').innerText = "0";
    return;
  }

  // Nível 6 é especial (OCR+Dividir)
  if (selectedLevel === 6) {
    const analyses = await Promise.all(selectedFiles.map(analisarPDF));
    let totalPages = analyses.reduce((sum, a) => sum + a.pages, 0);
    let totalSizeKB = analyses.reduce((sum, a) => sum + a.avgKbPerPage * a.pages, 0);
    let avgKbPerPage = totalPages > 0 ? totalSizeKB / totalPages : 100;

    // Limite real por volume: ~4.39 MB (MAX_DOC_MB_SAFE do backend)
    const MAX_VOLUME_MB = 4.39;
    const MAX_VOLUME_KB = MAX_VOLUME_MB * 1024;

    // Estimar compressão esperada por página baseado no tipo de conteúdo
    // PDFs escaneados (>300 KB/pág) comprimem ~60-70% com OCR+compressão
    // PDFs com imagens (<300 KB/pág) comprimem ~40-50%
    // PDFs leves (<100 KB/pág) quase não comprimem
    let compressionRatio;
    if (avgKbPerPage > 500) {
      compressionRatio = 0.25; // scans pesados: compressão de 75%
    } else if (avgKbPerPage > 300) {
      compressionRatio = 0.35; // scans médios
    } else if (avgKbPerPage > 150) {
      compressionRatio = 0.50; // misto imagem/texto
    } else if (avgKbPerPage > 80) {
      compressionRatio = 0.70; // texto com imagens leves
    } else {
      compressionRatio = 0.85; // texto puro, pouca compressão
    }

    const compressedKbPerPage = avgKbPerPage * compressionRatio;
    const pagesPerVolume = Math.floor(MAX_VOLUME_KB / compressedKbPerPage);
    const effectivePagesPerVol = Math.max(1, Math.min(pagesPerVolume, 30)); // entre 1 e 30 páginas
    const volumeCount = Math.ceil(totalPages / effectivePagesPerVol);

    // Tamanho estimado por volume (limitado a MAX_VOLUME_MB)
    const estSizePerVolume = Math.min(MAX_VOLUME_MB, (compressedKbPerPage * effectivePagesPerVol) / 1024);
    const totalEst = volumeCount * estSizePerVolume;

    document.getElementById('estSize').innerText = totalEst.toFixed(2);

    // Mostrar info útil: número de volumes esperados
    const redEl = document.getElementById('reduction');
    if (volumeCount > 1) {
      redEl.innerHTML = `<span title="${volumeCount} volumes estimados">${volumeCount} vols</span>`;
    } else {
      redEl.innerText = "OCR";
    }

    document.getElementById('configMapInput').value = JSON.stringify({ "0": selectedLevel });
    document.getElementById('extraCompressPagesInput').value = JSON.stringify(Array.from(extraCompressPages));
    return;
  }

  // Analisar todos os PDFs em paralelo
  const analyses = await Promise.all(selectedFiles.map(analisarPDF));

  // Calcular média ponderada de KB/página
  let totalPages = 0;
  let totalKB = 0;
  analyses.forEach(a => {
    totalPages += a.pages;
    totalKB += a.avgKbPerPage * a.pages;
  });
  const avgKbPerPage = totalPages > 0 ? totalKB / totalPages : 100;

  // Calcular multiplicador inteligente
  const mult = calcularMultiplicador(selectedLevel, avgKbPerPage, totalPages);
  const est = originalSizeMB * mult;

  document.getElementById('estSize').innerText = est.toFixed(2);

  const redEl = document.getElementById('reduction');
  const perc = originalSizeMB > 0 ? Math.max(0, (1 - mult) * 100) : 0;
  redEl.innerText = perc.toFixed(0);

  document.getElementById('configMapInput').value = JSON.stringify({ "0": selectedLevel });
  document.getElementById('extraCompressPagesInput').value = JSON.stringify(Array.from(extraCompressPages));
}

// Wrapper síncrono para compatibilidade
function atualizarEstimativa() {
  // Atualiza valores básicos imediatamente
  originalSizeMB = selectedFiles.reduce((sum, f) => sum + f.size / (1024 * 1024), 0);
  document.getElementById('origSize').innerText = originalSizeMB.toFixed(2);

  // Mostra "calculando..." enquanto analisa
  if (selectedFiles.length > 0) {
    const estEl = document.getElementById('estSize');
    const currentValue = estEl.innerText;
    if (currentValue === "0.00") {
      estEl.innerHTML = '<span class="text-muted" style="font-size:0.85em;">...</span>';
    }
  }

  // Executa análise assíncrona
  atualizarEstimativaAsync().catch(err => {
    console.warn('Erro na estimativa:', err);
    // Fallback para cálculo simples
    const multMap = { 1: 0.95, 2: 0.80, 3: 0.55, 4: 0.25, 5: 0.12, 6: 0.08 };
    const mult = multMap[selectedLevel] || 0.55;
    const est = originalSizeMB * mult;
    document.getElementById('estSize').innerText = est.toFixed(2);
    const perc = originalSizeMB > 0 ? Math.max(0, (1 - mult) * 100) : 0;
    document.getElementById('reduction').innerText = selectedLevel === 6 ? "OCR" : perc.toFixed(0);
  });

  document.getElementById('configMapInput').value = JSON.stringify({ "0": selectedLevel });
  document.getElementById('extraCompressPagesInput').value = JSON.stringify(Array.from(extraCompressPages));
}

/* =========================================================
   SUBMISSÃO E MONITORAMENTO SSE
   ========================================================= */
document.getElementById('mainForm').onsubmit = function(e) {
  e.preventDefault();
  if (selectedFiles.length === 0) return;

  const overlay = document.getElementById('progressOverlay');
  const progressBar = document.getElementById('progressBar');
  const statusText = document.getElementById('statusText');
  const logConsole = document.getElementById('logConsole');
  const elapsedText = document.getElementById('elapsedText');
  const pagesText = document.getElementById('pagesText');
  const percentText = document.getElementById('percentText');

  overlay.style.display = 'flex';
  logConsole.innerHTML = '<div class="text-info small mb-1">> Iniciando conexão...</div>';
  progressBar.style.width = '0%';
  document.getElementById('btnCancelar').style.display = 'inline-block';
  if (elapsedText) elapsedText.innerText = '0s';
  if (pagesText) pagesText.innerText = '0/0';
  if (percentText) percentText.innerText = '0%';

  const formData = new FormData();
  selectedFiles.forEach(f => formData.append('pdf', f));
  formData.append('config_map', JSON.stringify({ "0": selectedLevel }));
  formData.append('extra_compress_pages', JSON.stringify(Array.from(extraCompressPages)));

  const xhr = new XMLHttpRequest();
  currentXHR = xhr;

  xhr.upload.onprogress = (event) => {
    if (event.lengthComputable) {
      const percent = Math.round((event.loaded / event.total) * 100);
      const scaled = percent * 0.2;
      progressBar.style.width = scaled + '%';
      statusText.innerText = `Enviando arquivo(s): ${percent}%`;
      if (percentText) percentText.innerText = `${Math.round(scaled)}%`;
    }
  };

  xhr.onload = function() {
    if (xhr.status === 200) {
      const data = JSON.parse(xhr.responseText);
      if (data.task_id) {
        currentTaskId = data.task_id;
        statusText.innerText = "Processando no Servidor...";
        iniciarSSE(data.task_id, selectedFiles.length);
      }
    } else {
      statusText.innerText = "Erro no envio.";
      overlay.style.display = 'none';
      document.getElementById('btnCancelar').style.display = 'none';
    }
  };

  xhr.open('POST', '/processar');
  xhr.send(formData);
};

/* =========================================================
   DOWNLOAD
   ========================================================= */
async function baixarArquivo(url, nome) {
  const response = await fetch(url);
  if (!response.ok) throw new Error("Arquivo não encontrado ou erro no servidor.");
  const blob = await response.blob();
  const link = document.createElement('a');
  link.href = window.URL.createObjectURL(blob);
  link.download = nome || url.split('/').pop();
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  setTimeout(() => { try { URL.revokeObjectURL(link.href); } catch (e) {} }, 5000);
  return true;
}

function finalizarProcesso(url, nome) {
  isDownloading = true;
  const statusText = document.getElementById('statusText');
  const overlay = document.getElementById('progressOverlay');

  statusText.innerHTML = '<span class="text-success">✓</span> Pronto! Baixando...';
  document.getElementById('btnCancelar').style.display = 'none';

  setTimeout(async () => {
    try {
      await baixarArquivo(url, nome);
      if (typeof window.showCenterToast === "function") {
        window.showCenterToast({
          title: "Download concluído",
          message: "O arquivo foi baixado com sucesso.",
          type: "success",
          closeText: "Fechar",
          onClose: () => { if (overlay) overlay.style.display = 'none'; isDownloading = false; }
        });
      } else {
        alert("Download concluído ✅");
        if (overlay) overlay.style.display = 'none';
        isDownloading = false;
      }
      statusText.innerHTML = '<span class="text-success">✓</span> Download concluído.';
    } catch (err) {
      console.error(err);
      if (typeof window.showCenterToast === "function") {
        window.showCenterToast({
          title: "Falha no download",
          message: "Não foi possível baixar o arquivo. Tente novamente.",
          type: "error",
          closeText: "Fechar",
          onClose: () => { if (overlay) overlay.style.display = 'none'; isDownloading = false; }
        });
      } else {
        alert("Erro ao baixar arquivo: " + err.message);
        if (overlay) overlay.style.display = 'none';
        isDownloading = false;
      }
      statusText.innerHTML = '<span class="text-danger">✗</span> Erro ao baixar.';
    }
  }, 500);
}

/* =========================================================
   LIMPAR
   ========================================================= */
function limparTudo() {
  pdfInput.value = "";
  selectedFiles = [];
  originalSizeMB = 0;
  extraCompressPages = new Set();
  pdfAnalysisCache.clear();
  btnDownload.textContent = "OTIMIZAR AGORA";
  btnDownload.disabled = true;
  dropText.innerText = "Arraste PDFs aqui ou clique";

  const alertDiv = document.getElementById('alertAssinatura');
  if (alertDiv) { alertDiv.classList.add('d-none'); alertDiv.textContent = ''; }

  document.getElementById('origSize').innerText = "0.00";
  document.getElementById('estSize').innerText = "0.00";
  document.getElementById('reduction').innerText = "0";
  if (fileQueueEl) fileQueueEl.innerHTML = '';
  hidePagePanel();
}

btnClear.onclick = () => {
  if (selectedFiles.length === 0) return;
  if (confirm("Limpar irá remover todos os PDFs carregados. Continuar?")) limparTudo();
};

/* =========================================================
   SSE (Server-Sent Events) — progresso
   ========================================================= */
function iniciarSSE(taskId, fileCount) {
  const progressBar = document.getElementById('progressBar');
  const statusText = document.getElementById('statusText');
  const quickMessages = document.getElementById('quick-messages');
  const logConsole = document.getElementById('logConsole');
  const elapsedText = document.getElementById('elapsedText');
  const pagesText = document.getElementById('pagesText');
  const percentText = document.getElementById('percentText');
  const batchTracker = document.getElementById('batchTracker');
  const batchFileList = document.getElementById('batchFileList');
  const batchCounter = document.getElementById('batchCounter');
  const eventSource = new EventSource(`/progress/${taskId}`);
  currentEventSource = eventSource;
  let finalized = false;
  let pollingTimer = null;
  let alertAssinaturaMostrado = false;
  const isBatch = (fileCount || 1) > 1;
  let lastStatus = '';
  let lastStatusTs = Date.now();

  // Show/hide batch tracker
  if (batchTracker) batchTracker.style.display = isBatch ? 'block' : 'none';

  function renderBatchTracker(data) {
    if (!isBatch || !batchFileList) return;
    const names = data.file_names || [];
    const statuses = data.file_statuses || [];
    const fileIdx = data.file_index || 0;
    const total = data.file_count || names.length;
    const doneCount = statuses.filter(s => s === 'done').length;

    if (batchCounter) batchCounter.textContent = `${doneCount}/${total}`;

    batchFileList.innerHTML = names.map((name, i) => {
      const st = statuses[i] || 'pending';
      let icon, cls, extraInfo = '';
      if (st === 'done') {
        icon = '<span class="batch-icon done">&#10003;</span>';
        cls = 'batch-file-done';
      } else if (st === 'processing') {
        icon = '<span class="batch-icon processing"><span class="spinner-border spinner-border-sm text-primary" role="status"></span></span>';
        cls = 'batch-file-active';
        if (data.total > 0) {
          const pct = Math.round((data.current / data.total) * 100);
          extraInfo = `<div class="batch-file-bar"><div class="batch-file-bar-fill" style="width:${pct}%"></div></div>`;
        }
      } else {
        icon = '<span class="batch-icon pending">&#9679;</span>';
        cls = 'batch-file-pending';
      }
      const shortName = name.length > 30 ? name.substring(0, 27) + '...' : name;
      return `<div class="batch-file-item ${cls}">
        ${icon}
        <span class="batch-file-name" title="${name}">${shortName}</span>
        ${extraInfo}
      </div>`;
    }).join('');
  }

  const formatElapsed = (s) => {
    s = Math.max(0, Number(s) || 0);
    if (s < 60) return `${s}s`;
    return `${Math.floor(s / 60)}m ${s % 60}s`;
  };

  function buildQuickMessage(data, stalledSecs) {
    const dots = '.'.repeat((Math.floor(Date.now() / 700) % 3) + 1);
    const stage = data.stage || '';
    const detail = data.stage_detail || '';

    if (data.status === 'Concluído') return 'Concluído. Preparando download.';
    if (data.status === 'Cancelado') return 'Processamento cancelado pelo usuário.';
    if (data.status === 'Falha no processamento') return 'Falha no processamento. Verifique os logs.';

    if (stage === 'ocr_pages') {
      const curr = Number(data.current || 0);
      const total = Number(data.total || 0);
      return `OCR em andamento: ${curr}/${total} páginas${dots}`;
    }
    if (stage === 'page_compress') {
      return detail || `Ajustando compressão por página${dots}`;
    }
    if (stage === 'merge') {
      return `${detail || 'Mesclando páginas do OCR'}${dots} Etapa pesada, pode levar alguns minutos.`;
    }
    if (stage === 'extra_compress') {
      return `${detail || 'Aplicando compressão final'}${dots}`;
    }
    if (stage === 'split') {
      return detail || `Dividindo o PDF em volumes${dots}`;
    }
    if (stage === 'zip') {
      return detail || `Compactando arquivos finais${dots}`;
    }

    if (stalledSecs >= 12) {
      return `Ainda processando${dots} sem troca de etapa há ${stalledSecs}s (normal em arquivos grandes).`;
    }
    return detail || `Processando documento${dots}`;
  }

  function closeSSEAndPolling() {
    try { eventSource.close(); } catch (e) {}
    if (pollingTimer) {
      clearInterval(pollingTimer);
      pollingTimer = null;
    }
  }

  function handleTerminalStatus(data) {
    if (finalized) return;
    if (!data || !data.status) return;

    if (data.status === "Concluído") {
      finalized = true;
      closeSSEAndPolling();
      const finalUrl = `/download/${taskId}`;
      const nome = data.final_file || "resultado.pdf";
      currentTaskId = null;
      if (batchTracker) batchTracker.style.display = 'none';
      finalizarProcesso(finalUrl, nome);
      return;
    }
    if (data.status === "Cancelado") {
      finalized = true;
      closeSSEAndPolling();
      statusText.innerText = "Processamento Cancelado";
      progressBar.classList.add('bg-warning');
      document.getElementById('btnCancelar').style.display = 'none';
      if (batchTracker) batchTracker.style.display = 'none';
      currentTaskId = null;
      return;
    }
    if (data.status === "Falha no processamento") {
      finalized = true;
      closeSSEAndPolling();
      statusText.innerText = "Erro ao processar PDF";
      progressBar.classList.add('bg-danger');
      document.getElementById('btnCancelar').style.display = 'none';
      if (batchTracker) batchTracker.style.display = 'none';
      currentTaskId = null;
    }
  }

  eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);

    const now = Date.now();
    if (data.status !== lastStatus) {
      lastStatus = data.status || '';
      lastStatusTs = now;
    }
    const stalledSecs = Math.max(0, Math.floor((now - lastStatusTs) / 1000));

    // Calculate overall progress across all files
    const fc = data.file_count || 1;
    const fi = data.file_index || 0;
    const stagePercent = Number.isFinite(Number(data.stage_percent)) ? Number(data.stage_percent) : Number(data.percent || 0);
    let overallPercent;
    if (fc > 1 && fi > 0) {
      const completedFiles = (data.file_statuses || []).filter(s => s === 'done').length;
      const currentFileProgress = Math.max(0, Math.min(1, stagePercent / 100));
      overallPercent = ((completedFiles + currentFileProgress) / fc) * 100;
    } else {
      overallPercent = stagePercent;
    }
    const pctServidor = Math.max(0, Math.min(100, overallPercent));
    const progressoProcessamento = 20 + (pctServidor * 0.8);
    const isDone = data.status === 'Concluído';
    const visualPct = isDone ? 100 : Math.min(99, progressoProcessamento);
    const percentTotal = isDone ? 100 : Math.min(99, Math.round(visualPct));
    progressBar.style.width = visualPct + '%';
    if (percentText) percentText.innerText = `${percentTotal}%`;
    if (data.status) {
      const detail = data.stage_detail ? ` • ${data.stage_detail}` : '';
      statusText.innerText = `${data.status}${detail}`;
    }
    if (elapsedText && data.elapsed !== undefined) elapsedText.innerText = formatElapsed(data.elapsed);
    if (pagesText) {
      const stageLabel = data.stage_label || '';
      const total = Number(data.total || 0);
      if (total > 0) {
        pagesText.innerText = `${data.current || 0}/${data.total || 0}`;
      } else {
        pagesText.innerText = stageLabel || '--';
      }
    }
    if (quickMessages) quickMessages.innerText = buildQuickMessage(data, stalledSecs);

    // Update batch tracker
    renderBatchTracker(data);

    if (data.logs && data.logs.length > 0) {
      data.logs.forEach(msg => {
        const div = document.createElement('div');
        div.className = 'text-success small mb-1';
        div.innerHTML = `<span class="text-white-50">>></span> ${msg}`;
        logConsole.appendChild(div);
      });
      logConsole.scrollTop = logConsole.scrollHeight;
    }

    if (data.assinatura && !alertAssinaturaMostrado) {
      alertAssinaturaMostrado = true;
      const alertDiv = document.getElementById('alertAssinatura');
      alertDiv.textContent = 'Atenção: Este PDF possui uma assinatura digital.';
      alertDiv.classList.remove('d-none');
    }

    if (data.status === "Concluído") {
      handleTerminalStatus(data);
    }
    if (data.status === "Cancelado") {
      handleTerminalStatus(data);
    }
    if (data.status === "Falha no processamento") {
      handleTerminalStatus(data);
    }
  };

  eventSource.onerror = () => {
    if (finalized) return;
    // Fallback: consulta status final quando SSE cai no fim do processamento.
    if (!pollingTimer) {
      pollingTimer = setInterval(async () => {
        if (finalized) return;
        try {
          const resp = await fetch(`/progress_json/${taskId}`);
          if (!resp.ok) return;
          const data = await resp.json();
          handleTerminalStatus(data);
        } catch (e) {}
      }, 1500);
    }
  };
}

/* =========================================================
   GUARDAR NAVEGAÇÃO
   ========================================================= */
window.onbeforeunload = function() {
  const overlayVisible = document.getElementById('progressOverlay').style.display === 'flex';
  if (overlayVisible && !isDownloading) return "O processamento ainda está em curso.";
};

/* =========================================================
   CANCELAR
   ========================================================= */
document.getElementById('btnCancelar').onclick = async function() {
  if (!confirm('Tem certeza que deseja cancelar o processamento?')) return;
  const btn = document.getElementById('btnCancelar');
  btn.disabled = true; btn.textContent = 'Cancelando...';
  document.getElementById('statusText').innerText = 'Cancelando processamento...';

  if (currentTaskId) {
    try { await fetch(`/cancelar/${currentTaskId}`, { method: 'POST' }); } catch (err) { console.error(err); }
  }
  if (currentEventSource) { currentEventSource.close(); currentEventSource = null; }
  if (currentXHR) { currentXHR.abort(); currentXHR = null; }

  setTimeout(() => {
    document.getElementById('progressOverlay').style.display = 'none';
    btn.style.display = 'none'; btn.disabled = false; btn.textContent = 'Cancelar';

    const pb = document.getElementById('progressBar');
    pb.style.width = '0%'; pb.classList.remove('bg-danger', 'bg-warning');
    document.getElementById('statusText').innerText = 'Processando Documento';

    const el = document.getElementById('elapsedText');
    const pg = document.getElementById('pagesText');
    const pc = document.getElementById('percentText');
    if (el) el.innerText = '0s'; if (pg) pg.innerText = '0/0'; if (pc) pc.innerText = '0%';
    document.getElementById('logConsole').innerHTML = '<div class="text-success small mb-1">> Aguardando logs do servidor...</div>';
    currentTaskId = null;
  }, 1500);
};

/* =========================================================
   FEEDBACK
   ========================================================= */
(function () {
  const fbModal = document.getElementById("feedbackModal");
  const btnOpen = document.getElementById("openFeedback");
  const btnSend = document.getElementById("fbSend");
  const statusEl = document.getElementById("fbStatus");
  const msgEl = document.getElementById("fbMessage");
  const moduleEl = document.getElementById("fbModule");
  const starsWrap = document.getElementById("fbStars");
  if (!fbModal || !btnOpen || !btnSend || !starsWrap) return;

  let selectedStars = 0;

  function setStars(n) {
    selectedStars = n;
    starsWrap.querySelectorAll(".fb-star").forEach(b => b.classList.toggle("active", parseInt(b.dataset.star, 10) <= n));
  }

  function openModal() { fbModal.classList.remove("hidden"); fbModal.setAttribute("aria-hidden", "false"); if (statusEl) statusEl.textContent = ""; }
  function closeModal() { fbModal.classList.add("hidden"); fbModal.setAttribute("aria-hidden", "true"); }

  btnOpen.addEventListener("click", openModal);
  fbModal.addEventListener("click", (e) => { if (e.target?.dataset?.fbClose === "1") closeModal(); });
  starsWrap.addEventListener("click", (e) => { const b = e.target.closest(".fb-star"); if (b) setStars(parseInt(b.dataset.star, 10)); });

  btnSend.addEventListener("click", async () => {
    const message = (msgEl.value || "").trim();
    const module = (moduleEl.value || "").trim();
    if (selectedStars < 1 || selectedStars > 5) { if (statusEl) statusEl.textContent = "Selecione de 1 a 5 estrelas."; return; }
    if (message.length < 3) { if (statusEl) statusEl.textContent = "Escreva uma sugestão (mínimo 3 caracteres)."; return; }

    btnSend.disabled = true;
    if (statusEl) statusEl.textContent = "Enviando...";
    try {
      const r = await fetch("/feedback", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ stars: selectedStars, message, module }) });
      const data = await r.json().catch(() => ({}));
      if (!r.ok || !data.ok) { if (statusEl) statusEl.textContent = data?.error || "Falha ao enviar."; return; }
      if (statusEl) statusEl.textContent = "✅ Feedback enviado! Obrigado.";
      msgEl.value = ""; moduleEl.value = ""; setStars(0);
      setTimeout(closeModal, 800);
    } catch (err) { if (statusEl) statusEl.textContent = "Erro de conexão ao enviar feedback."; }
    finally { btnSend.disabled = false; }
  });
})();

/* =========================================================
   SELETOR DE PÁGINAS — Compressão Extra (PDF.js thumbnails)
   ========================================================= */
const pageCompressPanel = document.getElementById('pageCompressPanel');
const pageThumbnails = document.getElementById('pageThumbnails');
const THUMBS_PER_SLIDE = 100;
const THUMB_SCALE = 0.22;
const THUMB_RENDER_CONCURRENCY = 6;
const THUMB_PREFETCH_CONCURRENCY = 2;

let currentPdfDoc = null;
let currentPdfTotalPages = 0;
let currentThumbSlide = 1;
let thumbRenderToken = 0;
let thumbIsRendering = false;
let thumbCache = new Map();
let thumbRenderPromises = new Map();

function getSlideBounds(slide) {
  const startPage = ((slide - 1) * THUMBS_PER_SLIDE) + 1;
  const endPage = Math.min(startPage + THUMBS_PER_SLIDE - 1, currentPdfTotalPages);
  return { startPage, endPage };
}

async function getThumbDataUrl(pageNum) {
  const cached = thumbCache.get(pageNum);
  if (cached) return cached;

  if (thumbRenderPromises.has(pageNum)) {
    return thumbRenderPromises.get(pageNum);
  }

  const promise = (async () => {
    const page = await currentPdfDoc.getPage(pageNum);
    const viewport = page.getViewport({ scale: THUMB_SCALE });

    const canvas = document.createElement('canvas');
    canvas.className = 'page-thumb-canvas';
    canvas.width = Math.ceil(viewport.width);
    canvas.height = Math.ceil(viewport.height);

    const ctx = canvas.getContext('2d', { alpha: false });
    await page.render({ canvasContext: ctx, viewport }).promise;

    // JPEG reduz memória e acelera reaproveitamento entre blocos.
    const dataUrl = canvas.toDataURL('image/jpeg', 0.72);
    thumbCache.set(pageNum, dataUrl);
    return dataUrl;
  })();

  thumbRenderPromises.set(pageNum, promise);
  try {
    return await promise;
  } finally {
    thumbRenderPromises.delete(pageNum);
  }
}

function prefetchSlide(slide) {
  if (!currentPdfDoc || slide < 1) return;

  const totalSlides = Math.max(1, Math.ceil(currentPdfTotalPages / THUMBS_PER_SLIDE));
  if (slide > totalSlides) return;

  const { startPage, endPage } = getSlideBounds(slide);
  const pagesToPrefetch = [];

  for (let i = startPage; i <= endPage; i++) {
    if (!thumbCache.has(i) && !thumbRenderPromises.has(i)) {
      pagesToPrefetch.push(i);
    }
  }

  if (pagesToPrefetch.length === 0) return;

  let cursor = 0;
  const workers = Array.from({ length: Math.min(THUMB_PREFETCH_CONCURRENCY, pagesToPrefetch.length) }, async () => {
    while (cursor < pagesToPrefetch.length) {
      const idx = cursor++;
      const pageNum = pagesToPrefetch[idx];
      try {
        await getThumbDataUrl(pageNum);
      } catch (_) {
        // Falha em prefetch não deve interromper a experiência principal.
      }
    }
  });

  Promise.all(workers).catch(() => {});
}

function prefetchNeighborSlides(slide) {
  setTimeout(() => {
    prefetchSlide(slide + 1);
    prefetchSlide(slide - 1);
  }, 50);
}

function getThumbCarouselControls() {
  if (!pageCompressPanel || !pageThumbnails) return null;

  let controls = document.getElementById('thumbCarouselControls');
  if (controls) return controls;

  controls = document.createElement('div');
  controls.id = 'thumbCarouselControls';
  controls.className = 'thumb-carousel-controls d-none';
  controls.innerHTML = `
    <button type="button" class="thumb-carousel-btn" id="thumbPrevBtn" aria-label="Bloco anterior">&lsaquo; Anterior</button>
    <div class="thumb-carousel-meta">
      <div id="thumbSlideText" class="thumb-carousel-slide">Bloco 1/1</div>
      <div id="thumbRangeText" class="thumb-carousel-range">Páginas 1-1 de 1</div>
    </div>
    <button type="button" class="thumb-carousel-btn" id="thumbNextBtn" aria-label="Próximo bloco">Próximo &rsaquo;</button>
  `;

  pageCompressPanel.insertBefore(controls, pageThumbnails);

  controls.querySelector('#thumbPrevBtn')?.addEventListener('click', () => {
    renderThumbSlide(currentThumbSlide - 1);
  });
  controls.querySelector('#thumbNextBtn')?.addEventListener('click', () => {
    renderThumbSlide(currentThumbSlide + 1);
  });

  return controls;
}

function updateThumbCarouselStatus() {
  const controls = document.getElementById('thumbCarouselControls');
  if (!controls) return;

  const totalSlides = Math.max(1, Math.ceil(currentPdfTotalPages / THUMBS_PER_SLIDE));
  const prevBtn = controls.querySelector('#thumbPrevBtn');
  const nextBtn = controls.querySelector('#thumbNextBtn');
  const slideText = controls.querySelector('#thumbSlideText');
  const rangeText = controls.querySelector('#thumbRangeText');

  if (totalSlides <= 1) {
    controls.classList.add('d-none');
    return;
  }

  controls.classList.remove('d-none');
  const { startPage, endPage } = getSlideBounds(currentThumbSlide);

  if (slideText) slideText.textContent = `Bloco ${currentThumbSlide}/${totalSlides}`;
  if (rangeText) rangeText.textContent = `Páginas ${startPage}-${endPage} de ${currentPdfTotalPages}`;
  if (prevBtn) prevBtn.disabled = thumbIsRendering || currentThumbSlide <= 1;
  if (nextBtn) nextBtn.disabled = thumbIsRendering || currentThumbSlide >= totalSlides;
}

async function renderThumbSlide(targetSlide) {
  if (!pageThumbnails || !currentPdfDoc || currentPdfTotalPages < 1) return;

  const totalSlides = Math.max(1, Math.ceil(currentPdfTotalPages / THUMBS_PER_SLIDE));
  currentThumbSlide = Math.min(Math.max(1, targetSlide), totalSlides);

  thumbIsRendering = true;
  updateThumbCarouselStatus();

  const { startPage, endPage } = getSlideBounds(currentThumbSlide);

  const token = ++thumbRenderToken;
  pageThumbnails.innerHTML = '';

  const frag = document.createDocumentFragment();
  const thumbRefs = [];

  for (let i = startPage; i <= endPage; i++) {
    const wrapper = document.createElement('div');
    wrapper.className = 'page-thumb-item';
    wrapper.dataset.page = i;
    if (extraCompressPages.has(i)) wrapper.classList.add('selected');
    wrapper.onclick = () => togglePageSelection(i, wrapper);

    const img = document.createElement('img');
    img.className = 'page-thumb-image';
    img.alt = `Miniatura da página ${i}`;
    img.loading = 'lazy';

    const label = document.createElement('div');
    label.className = 'page-thumb-label';
    label.textContent = `Pág. ${i}`;

    wrapper.appendChild(img);
    wrapper.appendChild(label);
    frag.appendChild(wrapper);
    thumbRefs.push({ pageNum: i, img });
  }

  pageThumbnails.appendChild(frag);

  const notice = document.createElement('div');
  notice.className = 'text-muted small text-center py-2';
  notice.style.gridColumn = '1 / -1';
  notice.textContent = `Exibindo páginas ${startPage}-${endPage} de ${currentPdfTotalPages}`;
  pageThumbnails.appendChild(notice);

  try {
    let cursor = 0;
    const workers = Array.from({ length: Math.min(THUMB_RENDER_CONCURRENCY, thumbRefs.length) }, async () => {
      while (cursor < thumbRefs.length) {
        const idx = cursor++;
        const { pageNum, img } = thumbRefs[idx];

        try {
          const dataUrl = await getThumbDataUrl(pageNum);
          if (token !== thumbRenderToken) return;
          if (!img.isConnected) continue;
          img.src = dataUrl;
          img.classList.add('ready');
        } catch (_) {
          if (img.isConnected) {
            img.alt = `Falha ao carregar miniatura da página ${pageNum}`;
          }
        }
      }
    });

    await Promise.all(workers);
  } finally {
    if (token === thumbRenderToken) {
      thumbIsRendering = false;
      updateThumbCarouselStatus();
      prefetchNeighborSlides(currentThumbSlide);
    }
  }
}

function hidePagePanel() {
  if (pageCompressPanel) pageCompressPanel.style.display = 'none';
  if (pageThumbnails) pageThumbnails.innerHTML = '';
  const controls = document.getElementById('thumbCarouselControls');
  if (controls) controls.classList.add('d-none');

  currentPdfDoc = null;
  currentPdfTotalPages = 0;
  currentThumbSlide = 1;
  thumbIsRendering = false;
  thumbCache.clear();
  thumbRenderPromises.clear();
  thumbRenderToken += 1;
  extraCompressPages = new Set();
}

function togglePageSelection(pageNum, el) {
  if (extraCompressPages.has(pageNum)) {
    extraCompressPages.delete(pageNum);
    el.classList.remove('selected');
  } else {
    extraCompressPages.add(pageNum);
    el.classList.add('selected');
  }
  atualizarEstimativa();
}

async function renderPageThumbnails(file) {
  if (!pageThumbnails || !pageCompressPanel) return;

  const controls = getThumbCarouselControls();
  extraCompressPages = new Set();
  pageThumbnails.innerHTML = '<div class="text-muted small text-center py-3">Carregando pré-visualização...</div>';
  pageCompressPanel.style.display = 'block';

  try {
    const arrayBuffer = await file.arrayBuffer();
    const pdfjsLib = window['pdfjs-dist/build/pdf'] || window.pdfjsLib;
    pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';

    const pdf = await pdfjsLib.getDocument({ data: arrayBuffer }).promise;
    currentPdfDoc = pdf;
    currentPdfTotalPages = pdf.numPages;
    currentThumbSlide = 1;
    thumbCache.clear();
    thumbRenderPromises.clear();
    updateThumbCarouselStatus();

    await renderThumbSlide(1);
  } catch (err) {
    console.error('Erro ao renderizar thumbnails:', err);
    thumbCache.clear();
    thumbRenderPromises.clear();
    if (controls) controls.classList.add('d-none');
    pageThumbnails.innerHTML = '<div class="text-muted small text-center py-3">Não foi possível carregar pré-visualização</div>';
  }
}

// Botões Todas / Nenhuma
document.getElementById('btnSelectAll')?.addEventListener('click', () => {
  if (currentPdfTotalPages > 0) {
    extraCompressPages = new Set();
    for (let i = 1; i <= currentPdfTotalPages; i++) extraCompressPages.add(i);
    document.querySelectorAll('.page-thumb-item').forEach(el => el.classList.add('selected'));
  }
  atualizarEstimativa();
});

document.getElementById('btnSelectNone')?.addEventListener('click', () => {
  extraCompressPages = new Set();
  document.querySelectorAll('.page-thumb-item').forEach(el => el.classList.remove('selected'));
  atualizarEstimativa();
});
