const pdfjsLib = window['pdfjs-dist/build/pdf'];
pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.4.120/pdf.worker.min.js';

const pdfInput = document.getElementById('pdfInput');
const dropZone = document.getElementById('dropZone');
const dropText = document.getElementById('dropText');
const previewArea = document.getElementById('previewArea');
const btnDownload = document.getElementById('btnDownload');
const btnClear = document.getElementById('btnClear');

let pageConfigs = {};
let originalSizeMB = 0;
let totalPages = 0;
let isDownloading = false;
let currentPdfDoc = null;
let currentEventSource = null;
let currentXHR = null;

// LUPA
const modal = document.getElementById("pdfPreviewModal");
const canvas = document.getElementById("pdfPreviewCanvas");
const ctx = canvas ? canvas.getContext("2d") : null;

const titleEl = document.getElementById("pdfModalTitle");
const zoomInBtn = document.getElementById("zoomInBtn");
const zoomOutBtn = document.getElementById("zoomOutBtn");
const closeBtn = document.getElementById("closePdfModalBtn");

let currentPreviewPage = 1;
let currentScale = 1.6;

/* =========================================================
   ✅ BALÃO CENTRAL (Download concluído) - showCenterToast
   - fica no meio
   - só sai ao clicar em Fechar / clicar fora / ESC
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
      const fn = onCloseCb;
      onCloseCb = null;
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
      </div>
    `;

    container.classList.add("show");
    isOpen = true;
    document.body.style.overflow = "hidden";

    // Fechar ao clicar no fundo
    const backdrop = container.querySelector("[data-toast-close='1']");
    if (backdrop) backdrop.addEventListener("click", closeToast);

    // Fechar ao clicar no botão
    const closeBtn = container.querySelector("#toastCloseBtn");
    if (closeBtn) closeBtn.addEventListener("click", closeToast);

    // ESC
    const onKeyDown = (e) => {
      if (e.key === "Escape") {
        closeToast();
        document.removeEventListener("keydown", onKeyDown);
      }
    };
    document.addEventListener("keydown", onKeyDown);
  }

  window.showCenterToast = showCenterToast;
  window.closeCenterToast = closeToast;
})();

/* ===========================
   LUPA
   =========================== */

function openPdfModal() {
    if (!modal) return;
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
}

function closePdfModal() {
    if (!modal) return;
    modal.classList.add("hidden");
    modal.setAttribute("aria-hidden", "true");
    document.body.style.overflow = "";
}

if (modal) {
    modal.addEventListener("click", (e) => {
        if (e.target && e.target.dataset && e.target.dataset.close) closePdfModal();
    });
}

if (closeBtn) closeBtn.addEventListener("click", closePdfModal);

document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modal && !modal.classList.contains("hidden")) {
        closePdfModal();
    }
});

async function renderPreviewPage(pageNum) {
    if (!currentPdfDoc || !ctx || !canvas) {
        alert("PDF ainda não carregado para pré-visualização.");
        return;
    }

    const page = await currentPdfDoc.getPage(pageNum);
    const viewport = page.getViewport({ scale: currentScale });

    canvas.width = Math.floor(viewport.width);
    canvas.height = Math.floor(viewport.height);

    await page.render({ canvasContext: ctx, viewport }).promise;

    if (titleEl) {
        titleEl.textContent = `Página ${pageNum} • Zoom ${Math.round(currentScale * 100)}%`;
    }
}

if (zoomInBtn) {
    zoomInBtn.addEventListener("click", async () => {
        currentScale = Math.min(currentScale + 0.2, 4);
        await renderPreviewPage(currentPreviewPage);
    });
}

if (zoomOutBtn) {
    zoomOutBtn.addEventListener("click", async () => {
        currentScale = Math.max(currentScale - 0.2, 0.6);
        await renderPreviewPage(currentPreviewPage);
    });
}

// Clique na lupa (delegação)
document.addEventListener("click", async (e) => {
    const btn = e.target.closest("[data-action='zoom-page']");
    if (!btn) return;

    const pageNum = parseInt(btn.getAttribute("data-page"), 10);
    if (!pageNum) return;

    try {
        currentPreviewPage = pageNum;
        currentScale = 1.6;
        openPdfModal();
        await renderPreviewPage(pageNum);
    } catch (err) {
        console.error(err);
        alert("Não foi possível abrir a pré-visualização.");
        closePdfModal();
    }
});

// --- INTERATIVIDADE DA DROP ZONE ---
dropZone.onclick = () => pdfInput.click();
dropZone.ondragover = (e) => {
    e.preventDefault();
    dropZone.classList.add('drag-over');
    dropText.innerText = "Solte para carregar";
};
dropZone.ondragleave = () => {
    dropZone.classList.remove('drag-over');
    dropText.innerText = "Arraste o PDF aqui ou clique";
};
dropZone.ondrop = (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    const file = e.dataTransfer.files[0];
    if (file && file.type === "application/pdf") {
        pdfInput.files = e.dataTransfer.files;
        pdfInput.dispatchEvent(new Event('change'));
    }
};

// --- SELEÇÃO DE ARQUIVO E PREVIEW ---
pdfInput.onchange = async (e) => {
    const file = e.target.files[0];
    if (!file || file.type !== "application/pdf") return;

    // Verifica assinatura digital imediatamente
    const alertDiv = document.getElementById('alertAssinatura');
    alertDiv.classList.add('d-none');
    alertDiv.textContent = '';
    try {
        const formData = new FormData();
        formData.append('pdf', file);
        const resp = await fetch('/verificar-assinatura', { method: 'POST', body: formData });
        if (resp.ok) {
            const data = await resp.json();
            if (data.assinatura) {
                alertDiv.textContent = 'Atenção: Este PDF possui uma assinatura digital. Processos de compressão ou manipulação podem invalidar a assinatura.';
                alertDiv.classList.remove('d-none');
            }
        }
    } catch (err) {
        // Silencioso
    }

    previewArea.innerHTML = "";
    pageConfigs = {};
    currentPdfDoc = null;

    originalSizeMB = file.size / (1024 * 1024);
    document.getElementById('origSize').innerText = originalSizeMB.toFixed(2);
    dropText.innerText = file.name;
    btnDownload.disabled = true;
    btnDownload.textContent = "Carregando páginas...";

    let frasesEspera = setTimeout(() => {
        btnDownload.textContent = "Ainda carregando...";
        setTimeout(() => {
            btnDownload.textContent = "Quase lá...";
        }, 2000);
    }, 3000);

    const dt = new DataTransfer(); dt.items.add(file);
    document.getElementById('hiddenFile').files = dt.files;

    const reader = new FileReader();
    reader.onload = async function() {
        const typedarray = new Uint8Array(this.result);
        const pdf = await pdfjsLib.getDocument(typedarray).promise;

        currentPdfDoc = pdf;

        totalPages = pdf.numPages;
        let loadedPages = 0;
        for (let i = 0; i < totalPages; i++) {
            const skeleton = document.createElement('div');
            skeleton.className = "skeleton-card";
            skeleton.id = `page-container-${i}`;
            skeleton.innerHTML = `<div class="skeleton-img"></div><div style="height:12px;width:50%;background:#f1f5f9;margin:12px auto;border-radius:4px;"></div>`;
            previewArea.appendChild(skeleton);
            pageConfigs[i] = 3;
        }

        for (let i = 0; i < totalPages; i++) {
            await renderThumbnail(pdf, i);
            loadedPages++;
            if (loadedPages === totalPages) {
                clearTimeout(frasesEspera);
                btnDownload.textContent = "Pronto!";
                setTimeout(() => {
                    btnDownload.textContent = "OTIMIZAR AGORA";
                    btnDownload.disabled = false;
                }, 800);
            }
        }
        atualizarEstimativa();
    };
    reader.readAsArrayBuffer(file);
};

// --- RENDERIZAÇÃO E ESTIMATIVA ---
async function renderThumbnail(pdf, idx) {
    const page = await pdf.getPage(idx + 1);
    const viewport = page.getViewport({ scale: 0.4 });
    const container = document.getElementById(`page-container-${idx}`);
    const canvas = document.createElement('canvas');
    const context = canvas.getContext('2d');

    canvas.height = viewport.height;
    canvas.width = viewport.width;

    await page.render({ canvasContext: context, viewport: viewport }).promise;

    container.className = "page-card";
    container.innerHTML = `
        <div class="canvas-wrapper mb-3"></div>
        <div class="d-flex justify-content-between align-items-center mb-2">
            <span class="badge bg-light text-dark border">Pág. ${idx + 1}</span>

            <button type="button"
                class="preview-zoom-btn"
                data-action="zoom-page"
                data-page="${idx + 1}"
                title="Pré-visualizar esta página">
                <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24" class="me-1">
                    <path d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0zM10 7v6m-3-3h6"></path>
                </svg>
            </button>
        </div>
        <select class="form-select form-select-sm" data-idx="${idx}" onchange="updatePage(${idx}, this.value)">
            <option value="1">Padrão</option>
            <option value="2">Leve (HQ)</option>
            <option value="3" selected>Média (150dpi)</option>
            <option value="4">Alta (72dpi)</option>
            <option value="5">Muito Alta (50dpi)</option>
            <option value="6" disabled>OCR + Dividir</option>
        </select>`;
    container.querySelector('.canvas-wrapper').appendChild(canvas);
}

function updatePage(idx, val) { pageConfigs[idx] = parseInt(val); atualizarEstimativa(); }

function bulkApply(val) {
    document.querySelectorAll('.preview-grid select').forEach(s => {
        s.value = val;
        pageConfigs[s.dataset.idx] = parseInt(val);
    });
    atualizarEstimativa();
}

function atualizarEstimativa() {
    document.getElementById('configMapInput').value = JSON.stringify(pageConfigs);
    let fatorTotal = 0;
    const pesoPag = originalSizeMB / totalPages;
    let hasOCR = false;

    Object.values(pageConfigs).forEach(v => {
        let mult = 1.0;
        switch (v) {
            case 1: mult = 0.98; break;
            case 2: mult = 0.85; break;
            case 3: mult = 0.60; break;
            case 4: mult = 0.25; break;
            case 5: mult = 0.15; break;
            case 6: mult = 0.10; hasOCR = true; break;
        }
        fatorTotal += pesoPag * mult;
    });

    document.getElementById('estSize').innerText = fatorTotal.toFixed(2);
    const reductionElement = document.getElementById('reduction');
    if (hasOCR) {
        reductionElement.innerText = "OCR";
    } else {
        const perc = Math.max(0, ((1 - (fatorTotal / originalSizeMB)) * 100));
        reductionElement.innerText = perc.toFixed(0);
    }
}

// --- SUBMISSÃO E MONITORAMENTO SSE ---
document.getElementById('mainForm').onsubmit = function(e) {
    e.preventDefault();
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

    const xhr = new XMLHttpRequest();
    currentXHR = xhr;
    const formData = new FormData(e.target);

    // Monitoramento do Upload (0% a 20%)
    xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
            const percent = Math.round((event.loaded / event.total) * 100);
            const scaled = percent * 0.2;
            progressBar.style.width = scaled + '%';
            statusText.innerText = `Enviando arquivo: ${percent}%`;
            if (percentText) percentText.innerText = `${Math.round(scaled)}%`;
        }
    };

    xhr.onload = function() {
        if (xhr.status === 200) {
            const data = JSON.parse(xhr.responseText);
            if (data.task_id) {
                currentTaskId = data.task_id;
                statusText.innerText = "Processando no Servidor...";
                iniciarSSE(data.task_id);
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
   ✅ DOWNLOAD (agora retorna OK/ERRO sem alert interno)
   ========================================================= */
async function baixarArquivo(url, nome) {
    const response = await fetch(url);
    if (!response.ok) throw new Error("Arquivo não encontrado ou erro no servidor.");

    const blob = await response.blob(); // só termina quando baixou tudo
    const link = document.createElement('a');
    link.href = window.URL.createObjectURL(blob);
    link.download = nome || url.split('/').pop();
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    setTimeout(() => {
        try { URL.revokeObjectURL(link.href); } catch (e) {}
    }, 5000);

    return true;
}

/* =========================================================
   ✅ FINALIZA (mostra balão central após baixar)
   ========================================================= */
function finalizarProcesso(url, nome) {
    isDownloading = true;
    const statusText = document.getElementById('statusText');
    const overlay = document.getElementById('progressOverlay');

    statusText.innerHTML = '<span class="text-success">✓</span> Pronto! Baixando...';
    document.getElementById('btnCancelar').style.display = 'none';

    setTimeout(async () => {
        try {
            await baixarArquivo(url, nome);

            // ✅ BALÃO CENTRAL (fica até fechar)
            if (typeof window.showCenterToast === "function") {
                window.showCenterToast({
                    title: "Download concluído",
                    message: "O arquivo foi baixado com sucesso.",
                    type: "success",
                    closeText: "Fechar",
                    onClose: () => {
                        // ✅ se você quiser fechar o overlay só quando clicar em Fechar, deixe assim:
                        if (overlay) overlay.style.display = 'none';
                        isDownloading = false;
                    }
                });

                // Se preferir fechar overlay imediatamente e deixar só o balão, descomente:
                // if (overlay) overlay.style.display = 'none';
                // isDownloading = false;

            } else {
                // fallback caso não tenha o toast
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
                    onClose: () => {
                        if (overlay) overlay.style.display = 'none';
                        isDownloading = false;
                    }
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

btnClear.onclick = () => {
    if (dropText.innerText == "Arraste o PDF aqui ou clique") return;
    if (confirm("Limpar o formulário irá remover o arquivo carregado e todas as configurações feitas. Tem certeza que deseja continuar?")) {
        pdfInput.value = "";

        btnDownload.textContent = "OTIMIZAR AGORA";
        btnDownload.disabled = true;

        const alertAssinatura = document.getElementById('alertAssinatura');
        if (alertAssinatura) {
            alertAssinatura.classList.add('d-none');
            alertAssinatura.textContent = '';
        }

        previewArea.innerHTML = "";
        dropText.innerText = "Arraste o PDF aqui ou clique";

        pageConfigs = {};
        originalSizeMB = 0;
        totalPages = 0;
        currentPdfDoc = null;

        document.getElementById('origSize').innerText = "0.00";
        document.getElementById('estSize').innerText = "0.00";
        document.getElementById('reduction').innerText = "0";

        btnDownload.disabled = true;
        closePdfModal();
    }
};

function iniciarSSE(taskId) {
    const progressBar = document.getElementById('progressBar');
    const statusText = document.getElementById('statusText');
    const logConsole = document.getElementById('logConsole');
    const elapsedText = document.getElementById('elapsedText');
    const pagesText = document.getElementById('pagesText');
    const percentText = document.getElementById('percentText');
    const eventSource = new EventSource(`/progress/${taskId}`);
    currentEventSource = eventSource;
    let alertAssinaturaMostrado = false;

    const formatElapsed = (seconds) => {
        const s = Math.max(0, Number(seconds) || 0);
        if (s < 60) return `${s}s`;
        const m = Math.floor(s / 60);
        const r = s % 60;
        return `${m}m ${r}s`;
    };

    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        const progressoProcessamento = 20 + (data.percent * 0.8);
        const percentTotal = Math.min(100, Math.round(progressoProcessamento));
        progressBar.style.width = progressoProcessamento + '%';
        if (percentText) percentText.innerText = `${percentTotal}%`;

        if (data.status) {
            statusText.innerText = data.status;
        }

        if (elapsedText && data.elapsed !== undefined) {
            elapsedText.innerText = formatElapsed(data.elapsed);
        }

        if (pagesText && data.total !== undefined) {
            pagesText.innerText = `${data.current || 0}/${data.total || 0}`;
        }

        if (data.logs && data.logs.length > 0) {
            data.logs.forEach(msg => {
                const div = document.createElement('div');
                div.className = 'text-success small mb-1';
                div.innerHTML = `<span class="text-white-50">>></span> ${msg}`;
                logConsole.appendChild(div);
            });
            logConsole.scrollTop = logConsole.scrollHeight;
        }

        // ALERTA DE ASSINATURA DIGITAL
        if (data.assinatura && !alertAssinaturaMostrado) {
            alertAssinaturaMostrado = true;
            const alertDiv = document.getElementById('alertAssinatura');
            alertDiv.textContent = 'Atenção: Este PDF possui uma assinatura digital. Processos de compressão ou manipulação podem invalidar a assinatura.';
            alertDiv.classList.remove('d-none');
        }
        if (!data.assinatura && alertAssinaturaMostrado) {
            const alertDiv = document.getElementById('alertAssinatura');
            alertDiv.classList.add('d-none');
            alertDiv.textContent = '';
        }

        if (data.status === "Concluído") {
            eventSource.close();
            let finalUrl = `/download/${taskId}`;
            let nome = data.final_file || "resultado.pdf";
            currentTaskId = null;
            finalizarProcesso(finalUrl, nome);
        }

        if (data.status === "Cancelado") {
            eventSource.close();
            statusText.innerText = "Processamento Cancelado";
            progressBar.classList.add('bg-warning');
            document.getElementById('btnCancelar').style.display = 'none';
            currentTaskId = null;
        }

        if (data.status === "Falha no processamento") {
            eventSource.close();
            statusText.innerText = "Erro ao processar PDF";
            progressBar.classList.add('bg-danger');
            document.getElementById('btnCancelar').style.display = 'none';
            currentTaskId = null;
        }
    };

    eventSource.onerror = () => {
        eventSource.close();
    };
}

window.onbeforeunload = function() {
    const overlayVisible = document.getElementById('progressOverlay').style.display === 'flex';
    if (overlayVisible && !isDownloading) {
        return "O processamento ainda está em curso. Sair agora interromperá a tarefa.";
    }
};

// Botão de cancelar
let currentTaskId = null;

document.getElementById('btnCancelar').onclick = async function() {
    if (confirm('Tem certeza que deseja cancelar o processamento?')) {
        const btn = document.getElementById('btnCancelar');
        btn.disabled = true;
        btn.textContent = 'Cancelando...';
        document.getElementById('statusText').innerText = 'Cancelando processamento...';

        if (currentTaskId) {
            try {
                await fetch(`/cancelar/${currentTaskId}`, { method: 'POST' });
            } catch (err) {
                console.error('Erro ao cancelar no backend:', err);
            }
        }

        if (currentEventSource) {
            currentEventSource.close();
            currentEventSource = null;
        }

        if (currentXHR) {
            currentXHR.abort();
            currentXHR = null;
        }

        setTimeout(() => {
            document.getElementById('progressOverlay').style.display = 'none';
            btn.style.display = 'none';
            btn.disabled = false;
            btn.textContent = 'Cancelar';

            const progressBar = document.getElementById('progressBar');
            progressBar.style.width = '0%';
            progressBar.classList.remove('bg-danger');
            progressBar.classList.remove('bg-warning');

            document.getElementById('statusText').innerText = 'Processando Documento';
            const elapsedText = document.getElementById('elapsedText');
            const pagesText = document.getElementById('pagesText');
            const percentText = document.getElementById('percentText');
            if (elapsedText) elapsedText.innerText = '0s';
            if (pagesText) pagesText.innerText = '0/0';
            if (percentText) percentText.innerText = '0%';

            document.getElementById('logConsole').innerHTML = '<div class="text-success small mb-1">> Aguardando logs do servidor...</div>';

            currentTaskId = null;
        }, 1500);
    }
};

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
    const buttons = starsWrap.querySelectorAll(".fb-star");
    buttons.forEach((b) => {
      const v = parseInt(b.dataset.star, 10);
      b.classList.toggle("active", v <= n);
    });
  }

  function openModal() {
    fbModal.classList.remove("hidden");
    fbModal.setAttribute("aria-hidden", "false");
    if (statusEl) statusEl.textContent = "";
  }

  function closeModal() {
    fbModal.classList.add("hidden");
    fbModal.setAttribute("aria-hidden", "true");
  }

  btnOpen.addEventListener("click", openModal);

  fbModal.addEventListener("click", (e) => {
    if (e.target && e.target.dataset && e.target.dataset.fbClose === "1") closeModal();
  });

  starsWrap.addEventListener("click", (e) => {
    const btn = e.target.closest(".fb-star");
    if (!btn) return;
    setStars(parseInt(btn.dataset.star, 10));
  });

  btnSend.addEventListener("click", async () => {
    const message = (msgEl.value || "").trim();
    const module = (moduleEl.value || "").trim();

    if (selectedStars < 1 || selectedStars > 5) {
      if (statusEl) statusEl.textContent = "Selecione de 1 a 5 estrelas.";
      return;
    }
    if (message.length < 3) {
      if (statusEl) statusEl.textContent = "Escreva uma sugestão (mínimo 3 caracteres).";
      return;
    }

    btnSend.disabled = true;
    if (statusEl) statusEl.textContent = "Enviando...";

    try {
      const r = await fetch("/feedback", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stars: selectedStars, message, module })
      });
      const data = await r.json().catch(() => ({}));

      if (!r.ok || !data.ok) {
        if (statusEl) statusEl.textContent = (data && data.error) ? data.error : "Falha ao enviar.";
        return;
      }

      if (statusEl) statusEl.textContent = "✅ Feedback enviado! Obrigado.";
      msgEl.value = "";
      moduleEl.value = "";
      setStars(0);

      setTimeout(closeModal, 800);
    } catch (err) {
      if (statusEl) statusEl.textContent = "Erro de conexão ao enviar feedback.";
    } finally {
      btnSend.disabled = false;
    }
  });
})();