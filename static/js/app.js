// Weather Data Pipeline Dashboard JavaScript

// Global state
let refreshInterval = null;
let currentTab = 'weather';

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    setupEventListeners();
    startAutoRefresh();
});

function initializeDashboard() {
    // Load initial data
    updateStatus();
    loadWeatherData();
    updateLastUpdateTime();
}

function setupEventListeners() {
    // Tab switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            switchTab(this.dataset.tab);
        });
    });

    // Run pipeline button
    document.getElementById('runPipelineBtn').addEventListener('click', runPipeline);

    // Refresh button
    document.getElementById('refreshBtn').addEventListener('click', function() {
        refreshAll();
    });

    // Refresh logs button
    const refreshLogsBtn = document.getElementById('refreshLogsBtn');
    if (refreshLogsBtn) {
        refreshLogsBtn.addEventListener('click', loadLogs);
    }

    // Search functionality
    document.getElementById('searchInput').addEventListener('input', function(e) {
        filterWeatherCards(e.target.value);
    });

    // Terminal command buttons
    document.querySelectorAll('.terminal-cmd-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            executeCommand(this.dataset.cmd);
        });
    });

    // Clear terminal button
    const clearTerminalBtn = document.getElementById('clearTerminal');
    if (clearTerminalBtn) {
        clearTerminalBtn.addEventListener('click', function() {
            document.getElementById('terminalOutput').textContent = 'Terminal cleared.';
        });
    }
}

function switchTab(tabName) {
    currentTab = tabName;

    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });

    const tabMap = {
        'weather': 'weatherTab',
        'terminal': 'terminalTab',
        'logs': 'logsTab',
        'stats': 'statsTab'
    };

    document.getElementById(tabMap[tabName]).classList.add('active');

    // Load tab-specific data
    if (tabName === 'logs') {
        loadLogs();
    } else if (tabName === 'stats') {
        loadStats();
    }
}

async function updateStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();

        // Update status badge
        const statusElement = document.getElementById('pipelineStatus');
        const statusClass = `status-${data.status}`;
        const statusText = data.status.charAt(0).toUpperCase() + data.status.slice(1);
        
        statusElement.innerHTML = `<span class="status-badge ${statusClass}">${statusText}</span>`;

        // Update other status items
        document.getElementById('lastRun').textContent = data.last_run || 'Never';
        document.getElementById('recordsCount').textContent = data.records_processed || 0;
        document.getElementById('statusMessage').textContent = data.message || 'Ready';

        // Disable run button if running
        const runBtn = document.getElementById('runPipelineBtn');
        if (data.status === 'running') {
            runBtn.disabled = true;
            runBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Running...';
        } else {
            runBtn.disabled = false;
            runBtn.innerHTML = '<i class="fas fa-play"></i> Run Pipeline';
        }

    } catch (error) {
        console.error('Error updating status:', error);
    }
}

async function runPipeline() {
    try {
        const runBtn = document.getElementById('runPipelineBtn');
        runBtn.disabled = true;

        const response = await fetch('/api/run', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        const data = await response.json();

        if (data.success) {
            showToast('Pipeline started successfully', 'success');
            
            // Start polling for status updates
            const pollInterval = setInterval(async () => {
                await updateStatus();
                const statusResponse = await fetch('/api/status');
                const statusData = await statusResponse.json();
                
                if (statusData.status !== 'running') {
                    clearInterval(pollInterval);
                    await refreshAll();
                    
                    if (statusData.status === 'success') {
                        showToast('Pipeline completed successfully!', 'success');
                    } else if (statusData.status === 'error') {
                        showToast('Pipeline failed. Check logs for details.', 'error');
                    }
                }
            }, 2000);
        } else {
            showToast(data.message || 'Failed to start pipeline', 'error');
            runBtn.disabled = false;
        }

    } catch (error) {
        console.error('Error running pipeline:', error);
        showToast('Error starting pipeline', 'error');
        document.getElementById('runPipelineBtn').disabled = false;
    }
}

async function loadWeatherData() {
    const weatherCards = document.getElementById('weatherCards');
    weatherCards.innerHTML = '<div class="loading"><i class="fas fa-spinner fa-spin"></i> Loading weather data...</div>';

    try {
        const response = await fetch('/api/data');
        const result = await response.json();

        if (result.success) {
            // Update summary statistics
            if (result.summary) {
                document.getElementById('totalRecords').textContent = result.summary.total_records || 0;
                document.getElementById('avgTemp').textContent = `${result.summary.avg_temperature || '--'}°C`;
                document.getElementById('avgHumidity').textContent = `${result.summary.avg_humidity || '--'}%`;
                document.getElementById('citiesCount').textContent = result.summary.cities || 0;
            }

            // Get latest data for each city
            const latestResponse = await fetch('/api/data/latest');
            const latestResult = await latestResponse.json();

            if (latestResult.success && latestResult.data.length > 0) {
                displayWeatherCards(latestResult.data);
            } else {
                weatherCards.innerHTML = '<div class="loading"><i class="fas fa-info-circle"></i> No weather data available. Run the pipeline first.</div>';
            }
        } else {
            weatherCards.innerHTML = `<div class="loading"><i class="fas fa-exclamation-triangle"></i> ${result.message}</div>`;
        }

    } catch (error) {
        console.error('Error loading weather data:', error);
        weatherCards.innerHTML = '<div class="loading"><i class="fas fa-exclamation-triangle"></i> Error loading weather data</div>';
    }
}

function displayWeatherCards(data) {
    const weatherCards = document.getElementById('weatherCards');
    
    if (!data || data.length === 0) {
        weatherCards.innerHTML = '<div class="loading"><i class="fas fa-info-circle"></i> No data available</div>';
        return;
    }

    weatherCards.innerHTML = data.map(item => {
        const temp = Math.round(item.temperature || 0);
        const feelsLike = Math.round(item.feels_like || 0);
        const humidity = item.humidity || 0;
        const windSpeed = Math.round((item.wind_speed || 0) * 3.6); // Convert to km/h
        const description = item.weather_description || 'N/A';
        const city = item.city || 'Unknown';
        const country = item.country || '';

        return `
            <div class="weather-card" data-city="${city.toLowerCase()}">
                <div class="weather-header">
                    <div class="city-info">
                        <h3>${city}</h3>
                        <p>${country}</p>
                    </div>
                    <div class="temp-big">${temp}°C</div>
                </div>
                <div class="weather-condition">
                    <p style="font-size: 1.2rem; margin-bottom: 15px;">
                        <i class="fas fa-cloud"></i> ${description}
                    </p>
                </div>
                <div class="weather-details">
                    <div class="detail-item">
                        <i class="fas fa-temperature-high"></i>
                        <span>Feels Like: ${feelsLike}°C</span>
                    </div>
                    <div class="detail-item">
                        <i class="fas fa-tint"></i>
                        <span>Humidity: ${humidity}%</span>
                    </div>
                    <div class="detail-item">
                        <i class="fas fa-wind"></i>
                        <span>Wind: ${windSpeed} km/h</span>
                    </div>
                    <div class="detail-item">
                        <i class="fas fa-compress-arrows-alt"></i>
                        <span>Pressure: ${item.pressure || 0} hPa</span>
                    </div>
                </div>
            </div>
        `;
    }).join('');
}

function filterWeatherCards(searchTerm) {
    const cards = document.querySelectorAll('.weather-card');
    const term = searchTerm.toLowerCase();

    cards.forEach(card => {
        const city = card.dataset.city;
        if (city.includes(term)) {
            card.style.display = 'block';
        } else {
            card.style.display = 'none';
        }
    });
}

async function loadLogs() {
    const logsContent = document.getElementById('logsContent');
    logsContent.textContent = 'Loading logs...';

    try {
        const response = await fetch('/api/logs');
        const result = await response.json();

        if (result.success) {
            if (result.logs.length > 0) {
                logsContent.textContent = result.logs.join('');
                // Scroll to bottom
                logsContent.parentElement.scrollTop = logsContent.parentElement.scrollHeight;
            } else {
                logsContent.textContent = 'No logs available';
            }
        } else {
            logsContent.textContent = `Error: ${result.message}`;
        }

    } catch (error) {
        console.error('Error loading logs:', error);
        logsContent.textContent = 'Error loading logs';
    }
}

async function loadStats() {
    const statsDetails = document.getElementById('statsDetails');
    statsDetails.innerHTML = '<div class="loading"><i class="fas fa-spinner fa-spin"></i> Loading statistics...</div>';

    try {
        const response = await fetch('/api/stats');
        const result = await response.json();

        if (result.success) {
            const stats = result.stats;
            
            let html = '';

            // General stats
            html += `
                <div class="stats-section">
                    <h3><i class="fas fa-info-circle"></i> General Statistics</h3>
                    <div class="stats-row">
                        <span class="stats-label">Total Records</span>
                        <span class="stats-value">${stats.total_records || 0}</span>
                    </div>
                    <div class="stats-row">
                        <span class="stats-label">Unique Cities</span>
                        <span class="stats-value">${stats.cities_count || 0}</span>
                    </div>
                    <div class="stats-row">
                        <span class="stats-label">Countries</span>
                        <span class="stats-value">${stats.countries_count || 0}</span>
                    </div>
                </div>
            `;

            // Temperature stats
            if (stats.temperature) {
                html += `
                    <div class="stats-section">
                        <h3><i class="fas fa-thermometer-half"></i> Temperature Statistics</h3>
                        <div class="stats-row">
                            <span class="stats-label">Average</span>
                            <span class="stats-value">${stats.temperature.avg}°C</span>
                        </div>
                        <div class="stats-row">
                            <span class="stats-label">Minimum</span>
                            <span class="stats-value">${stats.temperature.min}°C</span>
                        </div>
                        <div class="stats-row">
                            <span class="stats-label">Maximum</span>
                            <span class="stats-value">${stats.temperature.max}°C</span>
                        </div>
                    </div>
                `;
            }

            // Humidity stats
            if (stats.humidity) {
                html += `
                    <div class="stats-section">
                        <h3><i class="fas fa-tint"></i> Humidity Statistics</h3>
                        <div class="stats-row">
                            <span class="stats-label">Average</span>
                            <span class="stats-value">${stats.humidity.avg}%</span>
                        </div>
                        <div class="stats-row">
                            <span class="stats-label">Minimum</span>
                            <span class="stats-value">${stats.humidity.min}%</span>
                        </div>
                        <div class="stats-row">
                            <span class="stats-label">Maximum</span>
                            <span class="stats-value">${stats.humidity.max}%</span>
                        </div>
                    </div>
                `;
            }

            // Cities list
            if (stats.cities && stats.cities.length > 0) {
                html += `
                    <div class="stats-section">
                        <h3><i class="fas fa-city"></i> Monitored Cities</h3>
                        <div class="stats-row">
                            <span class="stats-value">${stats.cities.join(', ')}</span>
                        </div>
                    </div>
                `;
            }

            statsDetails.innerHTML = html;

        } else {
            statsDetails.innerHTML = `<div class="loading"><i class="fas fa-exclamation-triangle"></i> ${result.message}</div>`;
        }

    } catch (error) {
        console.error('Error loading stats:', error);
        statsDetails.innerHTML = '<div class="loading"><i class="fas fa-exclamation-triangle"></i> Error loading statistics</div>';
    }
}

async function refreshAll() {
    await updateStatus();
    await loadWeatherData();
    updateLastUpdateTime();
    
    if (currentTab === 'logs') {
        await loadLogs();
    } else if (currentTab === 'stats') {
        await loadStats();
    }
    
    showToast('Data refreshed', 'info');
}

async function executeCommand(command) {
    const terminalOutput = document.getElementById('terminalOutput');
    
    // Show loading state
    terminalOutput.textContent = `$ ${command}\nExecuting...\n`;
    
    try {
        const response = await fetch('/api/execute', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ command: command })
        });
        
        const result = await response.json();
        
        if (result.success) {
            terminalOutput.textContent = `$ ${command}\n\n${result.output}`;
            showToast('Command executed successfully', 'success');
        } else {
            terminalOutput.textContent = `$ ${command}\n\nError: ${result.message || result.output}`;
            showToast('Command execution failed', 'error');
        }
        
    } catch (error) {
        console.error('Error executing command:', error);
        terminalOutput.textContent = `$ ${command}\n\nError: ${error.message}`;
        showToast('Error executing command', 'error');
    }
}

function startAutoRefresh() {
    // Refresh status every 5 seconds
    refreshInterval = setInterval(() => {
        updateStatus();
        updateLastUpdateTime();
    }, 5000);
}

function updateLastUpdateTime() {
    const now = new Date();
    const timeString = now.toLocaleTimeString();
    document.getElementById('lastUpdate').textContent = timeString;
}

function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = `toast ${type} show`;

    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
});
