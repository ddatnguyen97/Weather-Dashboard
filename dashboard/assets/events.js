(function () {
  const SELECTOR = "#global-date-picker input";
  let lastValue = null;

  function pushEvent(value) {
    if (value === lastValue) return;
    lastValue = value;

    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({
      event: "click_date_filter_btn",
      filter_value: value,
      debug_mode: true,
    });

    console.log("Event pushed:", value);
  }

  function initObserver(input) {
    input.addEventListener("input", () => pushEvent(input.value));

    new MutationObserver(() => pushEvent(input.value)).observe(input, {
      attributes: true,
      attributeFilter: ["value"],
    });

    console.log("Date input tracking initialized");
  }

  function waitForInput() {
    const input = document.querySelector(SELECTOR);
    if (input) {
      initObserver(input);
    } else {
      setTimeout(waitForInput, 300);
    }
  }

  document.addEventListener("DOMContentLoaded", waitForInput);
})();
