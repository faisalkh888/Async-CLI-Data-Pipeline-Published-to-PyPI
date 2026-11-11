const copyButton = document.querySelector('#copy-command');
const toast = document.querySelector('#toast');

copyButton.addEventListener('click', async () => {
  const command = 'uv sync --locked --extra dev';
  try {
    await navigator.clipboard.writeText(command);
    toast.textContent = 'Install command copied';
  } catch {
    toast.textContent = command;
  }
  toast.classList.add('show');
  window.setTimeout(() => toast.classList.remove('show'), 2200);
});

document.querySelectorAll('.nav-item').forEach((item) => {
  item.addEventListener('click', () => {
    document.querySelector('.nav-item.active')?.classList.remove('active');
    item.classList.add('active');
  });
});
