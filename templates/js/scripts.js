const pdfjsLib = window['pdfjs-dist/build/pdf'];
pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.4.120/pdf.worker.min.js';

const pdfInput = document.getElementById('pdfInput');
const dropZone = document.getElementById('dropZone');
const dropText = document.getElementById('dropText');
const previewArea = document.getElementById('previewArea');
const btnDownload = document.getElementById('btnDownload');

let pageConfigs = {};
let originalSizeMB = 0;
let totalPages = 0;

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

    previewArea.innerHTML = "";
    pageConfigs = {};

    originalSizeMB = file.size / (1024 * 1024);
    document.getElementById('origSize').innerText = originalSizeMB.toFixed(2);
    dropText.innerText = file.name;
    btnDownload.disabled = false;

    const dt = new DataTransfer(); dt.items.add(file);
    document.getElementById('hiddenFile').files = dt.files;

    const reader = new FileReader();
    reader.onload = async function() {
        const typedarray = new Uint8Array(this.result);
        const pdf = await pdfjsLib.getDocument(typedarray).promise;
        totalPages = pdf.numPages;
        
        for (let i = 0; i < totalPages; i++) {
            const skeleton = document.createElement('div');
            skeleton.className = "skeleton-card";
            skeleton.id = `page-container-${i}`;
            skeleton.innerHTML = `<div class="skeleton-img"></div><div style="height:12px;width:50%;background:#f1f5f9;margin:12px auto;border-radius:4px;"></div>`;
            previewArea.appendChild(skeleton);
            pageConfigs[i] = 3;
        }

        for (let i = 0; i < totalPages; i++) { await renderThumbnail(pdf, i); }
        atualizarEstimativa();
    };
    reader.readAsArrayBuffer(file);
};

// --- RENDERIZAÇÃO E ESTIMATIVA ---
async function renderThumbnail(pdf, idx) {
    const page = await pdf.getPage(idx + 1);
    const viewport = page.getViewport({ scale: 0.3 });
    const container = document.getElementById(`page-container-${idx}`);
    const canvas = document.createElement('canvas');
    await page.render({ canvasContext: canvas.getContext('2d'), viewport: viewport }).promise;

    container.className = "page-card";
    container.innerHTML = `
        <div class="canvas-wrapper mb-3 relative">
        <span class="fullscreen-icon" title="Visualizar em tela cheia" onclick="openFullscreen(this.parentElement)">
          
        </span>
        </div>

        <div class="d-flex justify-content-between align-items-center mb-2">
            <span class="badge bg-light text-dark border">Pág. ${idx + 1}</span>

        </div>
        <select class="form-select form-select-sm" data-idx="${idx}" onchange="updatePage(${idx}, this.value)">
            <option value="1">Padrão</option><option value="2">Leve (HQ)</option>
            <option value="3" selected>Média (150dpi)</option><option value="4">Alta (72dpi)</option>
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
    document.getElementById('reduction').innerText = hasOCR ? `OCR` : Math.max(0, ((1 - (fatorTotal / originalSizeMB)) * 100)).toFixed(0);
}

// --- SUBMISSÃO COM MONITOR DE UPLOAD REAL ---
document.getElementById('mainForm').onsubmit = function(e) {
    e.preventDefault();
    const overlay = document.getElementById('progressOverlay');
    const progressBar = document.getElementById('progressBar');
    const statusText = document.getElementById('statusText');
    const logConsole = document.getElementById('logConsole');

    overlay.style.display = 'flex';
    logConsole.innerHTML = ""; // Limpa o log para receber apenas mensagens do SSE

    const xhr = new XMLHttpRequest();
    const formData = new FormData(e.target);

    // 1. Monitoramento do Upload (0% a 20% da barra)
    xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
            const percent = Math.round((event.loaded / event.total) * 100);
            const scaled = percent * 0.2; // Upload representa os primeiros 20%
            progressBar.style.width = scaled + '%';
            statusText.innerText = `Enviando arquivo: ${percent}%`;
        }
    };

    xhr.onload = function() {
        if (xhr.status === 200) {
            const data = JSON.parse(xhr.responseText);
            if (data.task_id) {
                statusText.innerText = "Processando Documento";
                iniciarPolling(data);
            } else if (data.error) {
                statusText.innerText = "Erro: " + data.error;
            }
        } else {
            statusText.innerText = "Erro na comunicação com o servidor.";
        }
    };

    xhr.open('POST', '/processar');
    xhr.send(formData);
};

// No início do seu <script>, defina esta variável global
let isDownloading = false;
function iniciarPolling(data) {
    const overlay = document.getElementById('progressOverlay');
    const progressBar = document.getElementById('progressBar');
    const statusText = document.getElementById('statusText');
    const logConsole = document.getElementById('logConsole');
    
    // 1. Abre a conexão única com o servidor
    const eventSource = new EventSource(`/progress/${data.task_id}`);
    console.log(eventSource);

    eventSource.onmessage = function(event) {
        const payload = JSON.parse(event.data);

        // 2. Atualiza a barra de progresso (80% da barra é o processamento)
        const progressoCalculado = 20 + (payload.percent * 0.8);
        progressBar.style.width = progressoCalculado + '%';
        progressBar.innerText = Math.round(progressoCalculado) + '%';

        // 3. Atualiza o texto de status principal
        if (payload.status) {
            statusText.innerText = payload.status;
        }

        // 4. Adiciona as mensagens no console de logs
        if (payload.logs && payload.logs.length > 0) {
            payload.logs.forEach(msg => {
                const div = document.createElement('div');
                div.className = 'text-success small mb-1';
                div.innerHTML = `<span style="color: #666;">[${new Date().toLocaleTimeString()}]</span> ${msg}`;
                logConsole.appendChild(div);
            });
            // Auto-scroll para o final do log
            logConsole.scrollTop = logConsole.scrollHeight;
        }

        // 5. Finalização ou Erro
        if (payload.status === "Concluído") {
            eventSource.close();
            // Usa o nome do arquivo final enviado pelo backend
            const finalUrl = `/download/${data.task_id}/${payload.filename || 'resultado.pdf'}`;
            finalizarProcesso(finalUrl);
        } else if (payload.status === "Falha no processamento") {
            eventSource.close();
            statusText.innerText = "Erro no servidor";
            progressBar.classList.add('bg-danger');
        }
    };

    eventSource.onerror = () => eventSource.close();
}

function finalizarProcesso(url) {
    isDownloading = true; //
    document.getElementById('statusText').innerText = "Pronto! Iniciando download...";
    setTimeout(() => {
        window.location.href = url; //
        setTimeout(() => {
            document.getElementById('progressOverlay').style.display = 'none'; //
            isDownloading = false;
        }, 2000);
    }, 500);
}

// Segurança contra fechamento acidental corrigida
window.onbeforeunload = function() {
    const overlayVisible = document.getElementById('progressOverlay').style.display === 'flex';
    // Se estiver processando E não for a fase de download, avisa o usuário
    if (overlayVisible && !isDownloading) {
        return "O processamento ainda está em curso. Sair agora interromperá a tarefa.";
    }
};