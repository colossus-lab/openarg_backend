// Botón "copiar" en cada bloque de código. Sin dependencias: la CSP sólo
// permite scripts de este mismo origen.
document.querySelectorAll(".code").forEach((block) => {
  const code = block.querySelector("code");
  if (!code || !navigator.clipboard) return;
  const button = document.createElement("button");
  button.type = "button";
  button.className = "copy";
  button.textContent = "copiar";
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(code.innerText);
      button.textContent = "copiado";
    } catch {
      button.textContent = "no se pudo";
    }
    setTimeout(() => (button.textContent = "copiar"), 1800);
  });
  block.appendChild(button);
});
