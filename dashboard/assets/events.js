(function () {
  if (window._trackingInitialized) return;
  window._trackingInitialized = true;

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

  // function waitForDropdownElement(selector, callback) {
  //   const el = document.querySelector(selector);
  //   if (el) {
  //     callback(el);
  //   } else {
  //     setTimeout(() => waitForDropdownElement(selector, callback), 300);
  //   }
  // }

  // function trackDropdownSelection(containerSelector, eventName) {
  //   waitForDropdownElement(containerSelector, (container) => {
  //     let lastValue = "";

  //     const observer = new MutationObserver(() => {
  //       const labelEl = container.querySelector(".Select-value-label");
  //       if (!labelEl) return;

  //       const selectedValue = labelEl.textContent.trim();

  //       if (!selectedValue || selectedValue === lastValue) return;

  //       lastValue = selectedValue;
  //       pushEvent(eventName, { dropdown_value: selectedValue });
  //     });

  //     observer.observe(container, {
  //       childList: true,
  //       subtree: true,
  //       characterData: true,
  //     });

  //     console.log(`Dropdown tracking initialized for ${eventName}`);
  //   });
  // }

  function waitForDropdownElement(selector, callback) {
    const el = document.querySelector(selector);
    if (el) {
      callback(el);
    } else {
      setTimeout(() => waitForDropdownElement(selector, callback), 300);
    }
  }

  function debounce(fn, delay) {
    let timer;
    return function (...args) {
      clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), delay);
    };
  }

  function trackDropdownSelection(containerSelector, eventName) {
    waitForDropdownElement(containerSelector, (container) => {
      let lastValue = "";

      const labelEl = container.querySelector(".Select-value-label");
      if (!labelEl) {
        console.warn(`Label element not found in ${containerSelector}`);
        return;
      }

      const debouncedHandler = debounce(() => {
        const selectedValue = labelEl.textContent.trim();
        if (!selectedValue || selectedValue === lastValue) return;

        lastValue = selectedValue;
        pushEvent(eventName, { dropdown_value: selectedValue });
      }, 150);

      const observer = new MutationObserver(debouncedHandler);

      observer.observe(labelEl, {
        characterData: true,
        subtree: true,
        childList: true,
      });

      console.log(`Dropdown tracking initialized for ${eventName}`);
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    waitForInput("#global-date-picker input", "click_date_filter_btn");
    waitForSidebarLinks();

    trackDropdownSelection("#global-hour-picker", "change_hour_dropdown");
  });
})();
