document.addEventListener("DOMContentLoaded", function () {
  // 左侧黑色侧边栏控制
  var sidebar = document.getElementById("sidebarMenu");
  var sidebarToggle = document.getElementById("sidebarToggle");
  var sidebarClose = document.getElementById("sidebarClose");
  var sidebarBackdrop = document.getElementById("sidebarBackdrop");
  var sidebarBrand = document.querySelector(".sidebar-brand");

  // 移动端：默认侧边栏收起，只有点击按钮才展开
  var isMobile = window.innerWidth <= 768;
  var sidebarManuallyClosed = false;

  function toggleSidebar() {
    if (sidebar) {
      var isCollapsed = sidebar.classList.contains("collapsed");
      if (isCollapsed) {
        openSidebar();
      } else {
        closeSidebar();
      }
    }
  }

  function openSidebar() {
    if (sidebar) {
      sidebar.classList.remove("collapsed");
      if (sidebarBackdrop) {
        sidebarBackdrop.classList.add("active");
      }
      // 移动端才需要阻止滚动
      if (window.innerWidth <= 768) {
        document.body.style.overflow = "hidden";
      }
      // 用户主动打开，清除手动关闭标记
      if (isMobile) {
        sidebarManuallyClosed = false;
        sessionStorage.removeItem('sidebarManuallyClosed');
      }
    }
  }

  function closeSidebar() {
    if (sidebar) {
      sidebar.classList.add("collapsed");
      if (sidebarBackdrop) {
        sidebarBackdrop.classList.remove("active");
      }
      document.body.style.overflow = "";
      // 移动端：记录用户手动关闭了侧边栏（仅在当前页面内使用）
      if (isMobile) {
        sidebarManuallyClosed = true;
      }
    }
  }

  // 点击触发按钮打开侧边栏
  if (sidebarToggle) {
    sidebarToggle.addEventListener("click", function (e) {
      e.stopPropagation();
      e.preventDefault();
      openSidebar();
    });
  }

  // 点击关闭按钮关闭侧边栏
  if (sidebarClose) {
    sidebarClose.addEventListener("click", function (e) {
      e.stopPropagation();
      closeSidebar();
    });
  }

  // 点击"客户管理系统"标题收起/展开侧边栏
  if (sidebarBrand) {
    sidebarBrand.addEventListener("click", function (e) {
      e.stopPropagation();
      toggleSidebar();
    });
  }

  // 点击遮罩层关闭侧边栏（仅移动端）
  if (sidebarBackdrop) {
    sidebarBackdrop.addEventListener("click", function (e) {
      e.stopPropagation();
      closeSidebar();
    });
  }

  // 移动端：防止点击页面内容区域时打开侧边栏
  // 确保只有明确点击触发按钮时才打开
  document.addEventListener("click", function (e) {
    // 只在移动端且侧边栏已手动关闭时生效
    var currentIsMobile = window.innerWidth <= 768;
    if (currentIsMobile && sidebarManuallyClosed) {
      // 如果点击的不是侧边栏触发按钮，且侧边栏已手动关闭，则保持关闭状态
      if (e.target !== sidebarToggle && 
          sidebarToggle && 
          !sidebarToggle.contains(e.target) &&
          sidebar && 
          !sidebar.classList.contains("collapsed")) {
        // 如果侧边栏意外打开了，立即关闭
        closeSidebar();
      }
    }
  }, true);

  // 初始：在移动端默认收起侧边栏，避免每次进页面都自动弹出
  if (isMobile && sidebar) {
    closeSidebar();
  }

  // 确保所有操作按钮（如添加按钮）的点击不会打开侧边栏
  // 使用事件委托，确保动态添加的按钮也能生效
  document.addEventListener("click", function(e) {
    var currentIsMobile = window.innerWidth <= 768;
    // 检查点击的是否是操作按钮
    var btnAction = e.target.closest('.btn-action');
    if (btnAction && currentIsMobile && sidebarManuallyClosed) {
      e.stopPropagation();
      // 如果侧边栏已手动关闭，确保点击按钮时不会打开
      if (sidebar && !sidebar.classList.contains("collapsed")) {
        closeSidebar();
      }
    }
  }, true);

  // ESC 键关闭侧边栏
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && sidebar && !sidebar.classList.contains("collapsed")) {
      closeSidebar();
  }
  });

  // 客户管理：新增客户侧面板（遮罩层）
  var panel = document.querySelector("[data-customer-panel]");
  var openBtn = document.querySelector("[data-open-customer-panel]");
  var closeButtons = panel ? panel.querySelectorAll("[data-close-customer-panel]") : [];

  function openPanel() {
    if (panel) panel.hidden = false;
  }

  function closePanel() {
    if (panel) panel.hidden = true;
  }

  if (openBtn && panel) {
    openBtn.addEventListener("click", function () {
      openPanel();
    });
  }

  if (panel) {
    closeButtons.forEach(function (btn) {
      btn.addEventListener("click", closePanel);
    });

    panel.addEventListener("click", function (event) {
      if (event.target === panel) closePanel();
    });
  }

  // 一键复制联系方式
  document.addEventListener("click", function (e) {
    const btn = e.target.closest(".copy-phone-btn");
    if (!btn) return;

    const phone = btn.getAttribute("data-phone");
    if (!phone) return;

    e.preventDefault();
    e.stopPropagation();

    const showToast = function (text) {
      const toast = document.createElement("div");
      toast.className = "copy-toast";
      toast.textContent = text;
      document.body.appendChild(toast);
      requestAnimationFrame(function () {
        toast.classList.add("show");
      });
      setTimeout(function () {
        toast.classList.remove("show");
        setTimeout(function () {
          toast.remove();
        }, 200);
      }, 1200);
    };

    const fallbackCopy = function () {
      const temp = document.createElement("textarea");
      temp.value = phone;
      temp.style.position = "fixed";
      temp.style.opacity = "0";
      document.body.appendChild(temp);
      temp.select();
      try {
        document.execCommand("copy");
        showToast("已复制联系方式");
      } catch (err) {
        showToast("复制失败，请手动选择复制");
      }
      document.body.removeChild(temp);
    };

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard
        .writeText(phone)
        .then(function () {
          showToast("已复制联系方式");
        })
        .catch(function () {
          fallbackCopy();
        });
    } else {
      fallbackCopy();
    }
  });
});
