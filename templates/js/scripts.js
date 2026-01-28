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
let isDownloading = false;

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
    const viewport = page.getViewport({ scale: 0.6 });
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

    overlay.style.display = 'flex';
    logConsole.innerHTML = '<div class="text-info small mb-1">> Iniciando conexão...</div>'; 
    progressBar.style.width = '0%';

    const xhr = new XMLHttpRequest();
    const formData = new FormData(e.target);

    // Monitoramento do Upload (0% a 20%)
    xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
            const percent = Math.round((event.loaded / event.total) * 100);
            const scaled = percent * 0.2; 
            progressBar.style.width = scaled + '%';
            statusText.innerText = `Enviando arquivo: ${percent}%`;
        }
    };

    xhr.onload = function() {
        if (xhr.status === 200) {
            const data = JSON.parse(xhr.responseText);
            if (data.task_id) {
                statusText.innerText = "Processando no Servidor...";
                iniciarSSE(data.task_id); // Inicia o fluxo de logs em tempo real
            }
        } else {
            statusText.innerText = "Erro no envio.";
            overlay.style.display = 'none';
        }
    };

    xhr.open('POST', '/processar');
    xhr.send(formData);
};

async function baixarArquivo(url, nome) {
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error("Arquivo não encontrado ou erro no servidor.");
        const blob = await response.blob();
        const link = document.createElement('a');
        link.href = window.URL.createObjectURL(blob);
        link.download = nome || url.split('/').pop();
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    } catch (err) {
        alert("Erro ao baixar arquivo: " + err.message);
    }
}

function finalizarProcesso(url, nome) {
    isDownloading = true;
    const statusText = document.getElementById('statusText');
    statusText.innerHTML = '<span class="text-success">✓</span> Pronto! Baixando...';
    setTimeout(async () => {
        await baixarArquivo(url, nome);
        setTimeout(() => {
            document.getElementById('progressOverlay').style.display = 'none';
            isDownloading = false;
        }, 1000);
    }, 500);
}

btnClear.onclick = () => {
    if (dropText.innerText == "Arraste o PDF aqui ou clique") return;
    if (confirm("Limpar o formulário irá remover o arquivo carregado e todas as configurações feitas. Tem certeza que deseja continuar?")) {
        pdfInput.value = "";
        previewArea.innerHTML = "";
        dropText.innerText = "Arraste o PDF aqui ou clique";
        document.getElementById('origSize').innerText = "0.00";
        document.getElementById('estSize').innerText = "0.00";
        document.getElementById('reduction').innerText = "0";
        btnDownload.disabled = true;
    }
};

function iniciarSSE(taskId) {
    const progressBar = document.getElementById('progressBar');
    const statusText = document.getElementById('statusText');
    const logConsole = document.getElementById('logConsole');
    const eventSource = new EventSource(`/progress/${taskId}`);
    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        const progressoProcessamento = 20 + (data.percent * 0.8);
        progressBar.style.width = progressoProcessamento + '%';
        if (data.status) {
            statusText.innerText = data.status;
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
        if (data.status === "Concluído") {
            eventSource.close();
            // Usa o nome do arquivo final enviado pelo backend
            let finalUrl = `/download/${taskId}`;
            let nome = data.final_file || "resultado.pdf";
            finalizarProcesso(finalUrl, nome);
        }
        if (data.status === "Falha no processamento") {
            eventSource.close();
            statusText.innerText = "Erro ao processar PDF";
            progressBar.classList.add('bg-danger');
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