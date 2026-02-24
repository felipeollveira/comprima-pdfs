# 📄 PDF Optimizer
Uma aplicação web INTERNA para compressão de arquivos PDF. este sistema permite ao usuário definir níveis de qualidade **página por página**, garantindo que partes importantes mantenham a nitidez enquanto o restante do arquivo é otimizado.

---

## ✨ Funcionalidades

* **Interface Drag & Drop:** Arrastar e soltar PDFs para carregamento imediato.
* **Otimização Granular:** Seleção individual de DPI/Qualidade para cada página.
* **Lupa de Pré-visualização:** Modal com zoom dinâmico para conferir o conteúdo antes da compressão.
* **Detecção de Assinatura Digital:** Alerta preventivo que avisa se o PDF contém assinaturas que podem ser corrompidas durante o processo.
* **Monitoramento em Tempo Real (SSE):** Feedback visual do progresso no servidor via *Server-Sent Events*.
* **Estimativa de Tamanho:** Cálculo em tempo real da redução percentual com base nas configurações escolhidas.

---

## 🚀 Tecnologias

* **Frontend:** JavaScript (ES6+), HTML5, CSS3 (Bootstrap 5).
* **PDF Engine:** [PDF.js](https://mozilla.github.io/pdf.js/) (Mozilla) para renderização no lado do cliente.
* **Comunicação:** * `XMLHttpRequest` (Upload com progresso).
    * `Server-Sent Events` (Status de processamento assíncrono).

---

## 🛠️ Configurações de Qualidade

O sistema mapeia as escolhas do usuário para os seguintes perfis:

| Nível | DPI Sugerido | Multiplicador Est. | Uso Recomendado |
| :--- | :--- | :--- | :--- |
| **Padrão** | Original | 0.98x | Documentos legais e texto puro. |
| **Leve (HQ)** | 300 DPI | 0.85x | Portfólios e documentos com imagens. |
| **Média** | 150 DPI | 0.60x | Envio por e-mail e leitura em telas. |
| **Alta** | 72 DPI | 0.25x | Arquivamento histórico interno. |
| **Muito Alta** | 50 DPI | 0.15x | Máxima economia (thumbnails). |

---

## 💻 Fluxo de Execução Técnica

1.  **Carregamento:** O PDF é lido pelo `FileReader` e processado pelo `PDF.js` para gerar miniaturas (`canvas`).
2.  **Configuração:** O usuário altera os `selects`, disparando a função `atualizarEstimativa()`.
3.  **Envio:** O formulário envia o arquivo original e um JSON (`configMapInput`) com as instruções por página.
4.  **Processamento:** O backend processa o arquivo e envia logs de progresso via `EventSource`.
5.  **Finalização:** Ao atingir o status "Concluído", o frontend dispara o download automático do blob gerado.

---

## 🔧 Como Rodar o Projeto (Use linux)

1.  Clone o repositório:
    ```bash
    git clone https://github.com/dticsecad/pdf-otimizier
    ```
2.  **Requisito de Backend:** Este frontend espera os seguintes endpoints configurados:
    * `POST /verificar-assinatura`: Retorna `{ "assinatura": true/false }`.
    * `POST /processar`: Retorna `{ "task_id": "..." }`.
    * `GET /progress/{task_id}`: Stream de eventos SSE.
    * `GET /download/{task_id}`: Endpoint de download do arquivo final.
3. 

---
