// Weather Data Collector Spain Documentation JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // Add smooth scrolling for anchor links
    const anchorLinks = document.querySelectorAll('a[href^="#"]');
    
    anchorLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('href').substring(1);
            const targetElement = document.getElementById(targetId);
            
            if (targetElement) {
                targetElement.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
    
    // Add copy buttons to code blocks
    const codeBlocks = document.querySelectorAll('pre code');
    
    codeBlocks.forEach(block => {
        const container = block.parentElement;
        container.style.position = 'relative';
        
        const copyButton = document.createElement('button');
        copyButton.textContent = 'Copy';
        copyButton.className = 'copy-btn';
        copyButton.style.cssText = `
            position: absolute;
            top: 8px;
            right: 8px;
            background: #f0f0f0;
            border: 1px solid #ddd;
            border-radius: 3px;
            padding: 4px 8px;
            font-size: 12px;
            cursor: pointer;
            opacity: 0.7;
        `;
        
        copyButton.addEventListener('click', function() {
            navigator.clipboard.writeText(block.textContent).then(() => {
                copyButton.textContent = 'Copied!';
                setTimeout(() => {
                    copyButton.textContent = 'Copy';
                }, 2000);
            });
        });
        
        copyButton.addEventListener('mouseenter', function() {
            this.style.opacity = '1';
        });
        
        copyButton.addEventListener('mouseleave', function() {
            this.style.opacity = '0.7';
        });
        
        container.appendChild(copyButton);
    });
    
    // Add table of contents generation for long pages
    const headings = document.querySelectorAll('h2, h3, h4');
    if (headings.length > 3) {
        const toc = document.createElement('div');
        toc.className = 'table-of-contents';
        toc.innerHTML = '<h3>Table of Contents</h3>';
        
        const tocList = document.createElement('ul');
        
        headings.forEach((heading, index) => {
            const id = heading.id || `heading-${index}`;
            heading.id = id;
            
            const listItem = document.createElement('li');
            const link = document.createElement('a');
            link.href = `#${id}`;
            link.textContent = heading.textContent;
            link.className = `toc-${heading.tagName.toLowerCase()}`;
            
            listItem.appendChild(link);
            tocList.appendChild(listItem);
        });
        
        toc.appendChild(tocList);
        
        // Insert TOC after the first paragraph or h1
        const firstParagraph = document.querySelector('p');
        if (firstParagraph) {
            firstParagraph.parentNode.insertBefore(toc, firstParagraph.nextSibling);
        }
    }
    
    // Mobile navigation toggle
    const navToggle = document.querySelector('.nav-toggle');
    const navMenu = document.querySelector('.site-nav');
    
    if (navToggle && navMenu) {
        navToggle.addEventListener('click', function() {
            navMenu.classList.toggle('nav-open');
        });
    }
    
    // Add loading states for external links
    const externalLinks = document.querySelectorAll('a[href^="http"]:not([href*="' + window.location.hostname + '"])');
    
    externalLinks.forEach(link => {
        link.setAttribute('target', '_blank');
        link.setAttribute('rel', 'noopener noreferrer');
        
        // Add external link icon
        const icon = document.createElement('span');
        icon.innerHTML = ' ↗';
        icon.style.fontSize = '0.8em';
        icon.style.opacity = '0.6';
        link.appendChild(icon);
    });
});

// Table enhancement function
function enhanceTable(table) {
    // Make tables responsive
    const wrapper = document.createElement('div');
    wrapper.className = 'table-responsive';
    wrapper.style.overflowX = 'auto';
    
    table.parentNode.insertBefore(wrapper, table);
    wrapper.appendChild(table);
    
    // Add sorting functionality to headers
    const headers = table.querySelectorAll('th');
    headers.forEach((header, index) => {
        header.style.cursor = 'pointer';
        header.title = 'Click to sort';
        
        header.addEventListener('click', function() {
            sortTable(table, index);
        });
    });
}

function sortTable(table, columnIndex) {
    const tbody = table.querySelector('tbody') || table;
    const rows = Array.from(tbody.querySelectorAll('tr')).slice(1); // Skip header row
    
    const sortedRows = rows.sort((a, b) => {
        const aText = a.cells[columnIndex].textContent.trim();
        const bText = b.cells[columnIndex].textContent.trim();
        
        // Try to parse as numbers
        const aNum = parseFloat(aText);
        const bNum = parseFloat(bText);
        
        if (!isNaN(aNum) && !isNaN(bNum)) {
            return aNum - bNum;
        }
        
        // Fallback to string comparison
        return aText.localeCompare(bText);
    });
    
    // Remove existing rows and add sorted ones
    rows.forEach(row => row.remove());
    sortedRows.forEach(row => tbody.appendChild(row));
}

// Initialize table enhancements when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    const tables = document.querySelectorAll('table');
    tables.forEach(enhanceTable);
});
