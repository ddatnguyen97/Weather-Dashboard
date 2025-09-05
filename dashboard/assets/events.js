(function () {
  function pushEvent(eventName, value) {
    if (!value) return;

    const eventData = { event: eventName, debug_mode: true };

    if (typeof value === "object") {
      Object.assign(eventData, value); // spread object into event
    } else {
      eventData.value = value; // keep string value under "value"
    }

    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push(eventData);
    console.log("Event pushed:", eventData);
  }

  function initTracking(selector, eventName) {
    const input = document.querySelector(selector);
    if (!input) return;

    const handler = () => {
      const value = input.value;
      if (input._lastPushed === value) return;
      input._lastPushed = value;
      pushEvent(eventName, { filter_value: value });
    };

    input.addEventListener("change", handler);

    new MutationObserver(handler).observe(input, {
      attributes: true,
      attributeFilter: ["value"],
    });

    console.log(`Tracking initialized for ${eventName}`);
  }

  function waitForInput(selector, eventName) {
    const input = document.querySelector(selector);
    if (input) {
      initTracking(selector, eventName);
    } else {
      setTimeout(() => waitForInput(selector, eventName), 300);
    }
  }

  function trackAllSidebarLinks() {
    const links = document.querySelectorAll(".sidebar-link");
    links.forEach((link) => {
      if (link._tracked) return; // avoid duplicate listeners
      link._tracked = true;

      link.addEventListener("click", () => {
        pushEvent("click_sidebar_tab", {
          sidebar_id: link.id || null,
          sidebar_href: link.getAttribute("href") || null,
          sidebar_label: link.textContent.trim(),
        });
      });
    });
    console.log("Sidebar link tracking initialized");
  }

  function waitForSidebarLinks() {
    const links = document.querySelectorAll(".sidebar-link");
    if (links.length > 0) {
      trackAllSidebarLinks();
    } else {
      setTimeout(waitForSidebarLinks, 300);
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    waitForInput("#global-date-picker input", "click_date_filter_btn");
    waitForSidebarLinks();
  });
})();
