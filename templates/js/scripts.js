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
    dropZone.ondragover = (e) => { e.preventDefault(); dropZone.classList.add('drag-over'); dropText.innerText = "Solte para carregar"; };
    dropZone.ondragleave = () => { dropZone.classList.remove('drag-over'); dropText.innerText = "Arraste o PDF aqui ou clique"; };
    dropZone.ondrop = (e) => {
        e.preventDefault();
        dropZone.classList.remove('drag-over');
        const file = e.dataTransfer.files[0];
        if (file && file.type === "application/pdf") {
            pdfInput.files = e.dataTransfer.files;
            pdfInput.dispatchEvent(new Event('change'));
        }
    };


    pdfInput.onchange = async (e) => {
        const file = e.target.files[0];
        if (!file || file.type !== "application/pdf") return;

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

    async function renderThumbnail(pdf, idx) {
        const page = await pdf.getPage(idx + 1);
        const viewport = page.getViewport({ scale: 0.3 });
        const container = document.getElementById(`page-container-${idx}`);
        const canvas = document.createElement('canvas');
        await page.render({ canvasContext: canvas.getContext('2d'), viewport: viewport }).promise;

        container.className = "page-card";
        container.innerHTML = `
            <div class="canvas-wrapper mb-3"></div>
            <div class="d-flex justify-content-between align-items-center mb-2">
                <span class="badge bg-light text-dark border">Pág. ${idx + 1}</span>
            </div>
            <select class="form-select form-select-sm" data-idx="${idx}" onchange="updatePage(${idx}, this.value)">
                <option value="1">Padrão</option><option value="2">Leve (HQ)</option>
                <option value="3" selected>Média (150dpi)</option><option value="4">Alta (72dpi)</option>
                <option value="5">Muito Alta (50dpi)</option><option value="6" disabled>OCR</option>
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
        let isOCR = Object.values(pageConfigs).includes(6);
        document.getElementById('configMapInput').value = JSON.stringify(pageConfigs);
        if (isOCR) { 
            document.getElementById('estSize').innerText = "--"; 
            document.getElementById('reduction').innerText = "OCR"; 
            return; 
        }
        let fatorTotal = 0;
        const pesoPag = originalSizeMB / totalPages;
        Object.values(pageConfigs).forEach(v => {
            const mult = {1:0.98, 2:0.85, 3:0.60, 4:0.25, 5:0.15, 6:0.10}[v];
            fatorTotal += pesoPag * mult;
        });
        document.getElementById('estSize').innerText = fatorTotal.toFixed(2);
        document.getElementById('reduction').innerText = Math.max(0, ((1 - (fatorTotal / originalSizeMB)) * 100)).toFixed(0);
    }

    document.getElementById('mainForm').onsubmit = async (e) => {
        e.preventDefault();
        const overlay = document.getElementById('progressOverlay');
        const progressBar = document.getElementById('progressBar');
        overlay.style.display = 'flex';
        try {
            const response = await fetch('/processar', { method: 'POST', body: new FormData(e.target) });
            const data = await response.json();
            if (data.task_id) {
                const interval = setInterval(async () => {
                    const res = await fetch(`/status/${data.task_id}`);
                    const status = await res.json();
                    const p = Math.round((status.current / status.total) * 100) || 0;
                    progressBar.style.width = p + '%';
                    document.getElementById('statusText').innerText = status.status;
                    if (status.status === "Concluído") {
                        clearInterval(interval);
                        window.location.href = data.download_url;
                        setTimeout(() => { overlay.style.display = 'none'; progressBar.style.width = '0%'; }, 1000);
                    }
                }, 800);
            }
        } catch (err) { overlay.style.display = 'none'; }
    };