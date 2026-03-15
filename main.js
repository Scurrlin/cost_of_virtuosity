// Check if mobile
function isMobileDevice() {
    return window.innerWidth <= 450;
}

function initTableauViz(vizId) {
    // Show static images on mobile
    if (isMobileDevice()) {
        return;
    }
    
    const divElement = document.getElementById(vizId);
    if (divElement) {
        const vizElement = divElement.getElementsByTagName('object')[0];
        vizElement.style.width = '800px';
        vizElement.style.height = '627px';
        
        const scriptElement = document.createElement('script');
        scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
        vizElement.parentNode.insertBefore(scriptElement, vizElement);
    }
}

const dashboardIds = [
    'viz1757276102353',  // Acceptance Rates
    'viz1757276138501',  // Retention Rates
    'viz1757276208403',  // Graduation Rates
    'viz1760800365068',  // Acceptance Rates 2022
    'viz1760800437240',  // Retention Rates 2022
    'viz1760800715935',  // Graduation Rates 2022
    'viz1757276451152',  // Tuition w/o Curtis
    'viz1757276474042',  // Tuition (All institutions)
    'viz1757276493141',  // Average Net Price
    'viz1760800044820',  // Tuition Cost 2022
    'viz1760799814627',  // Average Net Price 2022
    'viz1757276660099',  // Enrollment w/o Berklee
    'viz1757276680506',  // Enrollment (All institutions)
    'viz1757282252334'   // Total Enrollment 2022
];

function initAllTableauViz() {
    dashboardIds.forEach(function(id) {
        initTableauViz(id);
    });
}

function copyCode(button, codeId) {
    const codeElement = document.getElementById(codeId);
    if (codeElement) {
        const code = codeElement.textContent;
        
        navigator.clipboard.writeText(code).then(function() {
            const originalText = button.innerHTML;
            button.innerHTML = '<svg class="copy-icon" viewBox="0 0 24 24"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>Copied!';
            button.classList.add('copied');
            
            setTimeout(function() {
                button.innerHTML = originalText;
                button.classList.remove('copied');
            }, 2000);
        }).catch(function(err) {
            console.error('Failed to copy text: ', err);
            const textArea = document.createElement('textarea');
            textArea.value = code;
            document.body.appendChild(textArea);
            textArea.select();
            document.execCommand('copy');
            document.body.removeChild(textArea);
            
            const originalText = button.innerHTML;
            button.innerHTML = '<svg class="copy-icon" viewBox="0 0 24 24"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>Copied!';
            button.classList.add('copied');
            
            setTimeout(function() {
                button.innerHTML = originalText;
                button.classList.remove('copied');
            }, 2000);
        });
    }
}

document.addEventListener('DOMContentLoaded', function() {
    initAllTableauViz();
});