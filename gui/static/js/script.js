document.addEventListener('DOMContentLoaded', () => {
    const configForm = document.getElementById('config-form');
    const knownAdaptersInput = document.getElementById('known_adapters_filename');
    const logOutput = document.getElementById('log-output');
    
    // --- Initial State Loading ---
    
    // 1. Load and display the current configuration
    const loadConfig = async () => {
        const response = await fetch('/config');
        const config = await response.json();
        knownAdaptersInput.value = config.known_adapter_filename;
    };

    // 2. Load and display the run history
    const loadHistory = async () => {
        const response = await fetch('/history');
        const history = await response.json();
        document.querySelectorAll('.step').forEach(stepEl => {
            stepEl.classList.remove('completed', 'running', 'failed'); // Reset all
            const stepName = stepEl.id.replace('step-', '');
            if (history.includes(stepName)) {
                stepEl.classList.add('completed');
            }
        });
    };
    
    // --- Event Handling ---

    // 3. Handle saving the configuration
    configForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const response = await fetch('/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 'known_adapter_filename': knownAdaptersInput.value })
        });
        const result = await response.json();
        alert(result.message);
    });
    
    // 4. Handle clicks on the "Run" buttons
    document.querySelectorAll('.run-btn').forEach(button => {
        button.addEventListener('click', async () => {
            const step = button.dataset.step;
            logOutput.textContent = `Attempting to run pipeline from step: ${step}...\n`;
            
            await fetch('/run', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ start_step: step })
            });
        });
    });

    // 5. Handle "Clear History" button
    document.getElementById('clear-history-btn').addEventListener('click', async () => {
        if (confirm('Are you sure you want to clear the run history?')) {
            await fetch('/history', { method: 'POST' });
            loadHistory(); // Refresh UI
        }
    });

    // 6. Listen for Server-Sent Events (SSE) for live logs
    const eventSource = new EventSource('/stream');
    eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);
        logOutput.textContent += `${data.message}\n`;
        logOutput.scrollTop = logOutput.scrollHeight; // Auto-scroll

        // Update step status based on log messages
        if (data.message.includes('Executing pipeline step:')) {
            const stepName = data.message.split(': ')[1].trim();
            const stepEl = document.getElementById(`step-${stepName}`);
            if (stepEl) {
                stepEl.classList.remove('completed', 'failed');
                stepEl.classList.add('running');
            }
        } else if (data.message.includes('SUCCESS: Completed step')) {
             const stepName = data.message.split('step ')[1].trim();
             const stepEl = document.getElementById(`step-${stepName}`);
             if (stepEl) {
                stepEl.classList.remove('running', 'failed');
                stepEl.classList.add('completed');
             }
        } else if (data.level === 'ERROR') {
             document.querySelectorAll('.step.running').forEach(el => el.classList.add('failed'));
        } else if (data.level === 'COMPLETE') {
            loadHistory(); // Final refresh when pipeline finishes
        }
    };
    
    // --- Initial Page Load ---
    loadConfig();
    loadHistory();
});