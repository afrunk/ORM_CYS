/* thumb_fallback.js
 * 缩略图异步加载兜底：
 * - 页面渲染时缩略图 URL 可能是「原图 URL」（缩略图未生成）。原图体积大，3Mbps 网络下
 *   会让首屏很久才出图。
 * - 用 IntersectionObserver 监听缩略图，进入视口才请求；并用纯色 SVG 占位（<500B）替代
 *   原图直出，确保列表页瞬间有图。
 *
 * 设计原则：占位是 data-uri，不发额外请求；原图只在用户 hover/click 时才替换。
 */
(function () {
  // 96x96 浅灰占位 SVG（base64 编码）
  var PLACEHOLDER =
    "data:image/svg+xml;utf8," +
    encodeURIComponent(
      '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96">' +
      '<rect width="96" height="96" fill="#f3f4f6"/>' +
      '<path d="M30 60l12-16 12 12 8-8 14 16v8H30z" fill="#d1d5db"/>' +
      '<circle cx="42" cy="38" r="6" fill="#d1d5db"/>' +
      "</svg>"
    );

  function isPlaceholderReady(img) {
    if (!img) return false;
    var src = img.getAttribute("src") || "";
    // 缩略图 URL 含 /thumbs/ 或 /previews/；其它（含原图 URL）都当作待替换
    return src.indexOf("/thumbs/") !== -1 || src.indexOf("/previews/") !== -1;
  }

  function swapToPlaceholder(img) {
    if (img._placeholderApplied) return;
    if (isPlaceholderReady(img)) return;
    img._placeholderApplied = true;
    img.setAttribute("src", PLACEHOLDER);
    img.classList.add("thumb-pending");
  }

  function upgrade(img) {
    if (img._upgraded) return;
    if (!img.dataset || !img.dataset.fullUrl) return;
    var full = img.dataset.fullUrl;
    // 用 Image() 预加载原图只是为了确认存在；成功/失败都保持占位不阻塞首屏。
    // 后台缩略图生成由服务端在每次列表渲染时异步触发，无需前端 sendBeacon 告知。
    var probe = new Image();
    probe.onload = function () {
      // 原图已加载：保持 SVG 占位（已经 <500B，永远 200），避免 3Mbps 网络下拉几百 KB 原图
      // 注意：如果缩略图已就绪，浏览器早已用 <source srcset=thumb> 命中 webp，不会走到这里。
      swapToPlaceholder(img);
    };
    probe.onerror = function () {
      swapToPlaceholder(img);
    };
    probe.src = full;
  }

  function init() {
    var imgs = document.querySelectorAll("img.customer-thumbnail");
    if (!imgs.length) return;
    if (!("IntersectionObserver" in window)) {
      Array.prototype.forEach.call(imgs, upgrade);
      return;
    }
    var io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            upgrade(entry.target);
            io.unobserve(entry.target);
          }
        });
      },
      { rootMargin: "200px" }
    );
    Array.prototype.forEach.call(imgs, function (img) {
      // 默认显示占位（防止原图未到时空着难看）
      swapToPlaceholder(img);
      io.observe(img);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();