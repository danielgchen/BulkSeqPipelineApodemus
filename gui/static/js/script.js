document.addEventListener('DOMContentLoaded', function() {
    const form = document.getElementById('config-form');
    const runButton = document.getElementById('run-button');
    const logOutput = document.getElementById('log-output');
    const flowchartContainer = document.getElementById('flowchart');
    const modal = document.getElementById('message-modal');
    const modalMessage = document.getElementById('modal-message');

    // --- Define Pipeline Modules ---
    const modules = [
        "qc_raw_fastq",
        "detect_adapters",
        "quantify_adapters",
        "trim_fastq",
        "qc_trimmed_fastq",
        "map_fastq_to_bam",
        "index_bam",
        "dedup_bam",
        "index_dedup_bam",
        "qc_nondedup_bam",
        "aggregate_counts",
        "aggregate_qc_reports",
    ];

    // --- Initialize Flowchart ---
    function initializeFlowchart() {
        flowchartContainer.innerHTML = ''; // Clear previous state
        modules.forEach((module, index) => {
            const node = document.createElement('div');
            node.id = `node-${module}`;
            node.className = 'flowchart-node w-4/5 text-center p-3 rounded-lg border-2 border-gray-300 bg-gray-100 text-gray-500 font-medium';
            node.textContent = module.replace(/_/g, ' ');
            flowchartContainer.appendChild(node);

            if (index < modules.length - 1) {
                const arrow = document.createElement('div');
                arrow.className = 'flowchart-arrow';
                flowchartContainer.appendChild(arrow);
            }
        });
    }

    initializeFlowchart(); // Initial setup

    // --- Handle Form Submission ---
    form.addEventListener('submit', async function(event) {
        event.preventDefault();
        runButton.disabled = true;
        runButton.textContent = 'Running...';
        runButton.classList.add('opacity-50', 'cursor-not-allowed');

        // Reset UI for new run
        initializeFlowchart();
        logOutput.textContent = '';

        // Collect form data, using placeholder if value is empty
        const formData = new FormData(form);
        const data = {};
        formData.forEach((value, key) => {
            const input = form.elements[key];
            data[key] = value.trim() || input.placeholder;
        });

        try {
            const response = await fetch('/run', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data),
            });

            const result = await response.json();
            if (result.status === 'success') {
                // Start polling for status updates
                startPolling();
            } else {
                showModal(`Error: ${result.message}`);
                resetRunButton();
            }
        } catch (error) {
            showModal(`Network Error: ${error.message}`);
            resetRunButton();
        }
    });

    let pollingInterval;

    function startPolling() {
        // Clear any existing interval
        if (pollingInterval) {
            clearInterval(pollingInterval);
        }
        // Poll every 2 seconds for faster updates
        pollingInterval = setInterval(fetchStatus, 2000);
    }

    async function fetchStatus() {
        try {
            const response = await fetch('/status');
            const data = await response.json();
            const logContent = data.log_content;
            const statusContent = data.status_content;
            
            logOutput.textContent = logContent;
            logOutput.scrollTop = logOutput.scrollHeight; // Auto-scroll to bottom

            updateFlowchart(statusContent);

            // Stop polling if the pipeline is finished
            if (statusContent.includes("INFO: Pipeline finished.")) {
                clearInterval(pollingInterval);
                resetRunButton();
            }
        } catch (error) {
            console.error('Error fetching status:', error);
            clearInterval(pollingInterval); // Stop on error
            resetRunButton();
        }
    }

    function updateFlowchart(statusContent) {
        const lines = statusContent.split('\n');
        lines.forEach(line => {
            if (line.startsWith('STATUS:')) {
                const parts = line.split(' ');
                const moduleName = parts[1];
                const status = parts[2];
                const node = document.getElementById(`node-${moduleName}`);
                if (node) {
                    updateNodeStyle(node, status);
                }
            }
        });
    }

    function updateNodeStyle(node, status) {
        // remove all status-related classes first
        node.classList.remove('bg-gray-100', 'border-gray-300', 'text-gray-500', 'bg-blue-100', 'border-blue-500', 'text-blue-800', 'bg-green-100', 'border-green-500', 'text-green-800', 'bg-yellow-100', 'border-yellow-500', 'text-yellow-800');
        // remove all previous children
        const childrenToRemove = node.querySelectorAll('img');
        childrenToRemove.forEach(child => { child.remove() });
        
        switch (status) {
            case 'in_progress':
                node.classList.add('bg-blue-100', 'border-blue-500', 'text-blue-800');
                var elem = document.createElement("img");
                elem.src = "static/img/gerbil_17081647.favicon.INPROGRESS.png";
                elem.alt = "gerbil indicating step is in progress"
                elem.classList.add('inline-block', 'height=100px');
                node.appendChild(elem);
                break;
            case 'finished':
                node.classList.add('bg-green-100', 'border-green-500', 'text-green-800');
                var elem = document.createElement("img");
                elem.src = "static/img/experiment_1360747.favicon.COMPLETED.png";
                elem.alt = "mouse with checkmark indicating step is completed"
                elem.classList.add('inline-block', 'height=100px');
                node.appendChild(elem);
                break;
            case 'skipped':
                node.classList.add('bg-yellow-100', 'border-yellow-500', 'text-yellow-800');
                var elem = document.createElement("img");
                elem.src = "static/img/mouse_4786662.favicon.SKIPPED.png";
                elem.alt = "mouse indicating step is skipped"
                elem.classList.add('inline-block', 'height=100px');
                node.appendChild(elem);
                break;
            default:
                node.classList.add('bg-gray-100', 'border-gray-300', 'text-gray-500');
                break;
        }
    }

    function resetRunButton() {
        runButton.disabled = false;
        runButton.textContent = 'Run Pipeline';
        runButton.classList.remove('opacity-50', 'cursor-not-allowed');
    }

    function showModal(message) {
        modalMessage.textContent = message;
        modal.classList.remove('hidden');
    }
});