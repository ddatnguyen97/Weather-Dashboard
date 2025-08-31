window.dataLayer = window.dataLayer || [];
function gtag() {
  dataLayer.push(arguments);
}

gtag("js", new Date());
gtag("config", "G-SBGP0H0LEN");

function pushDateFilterEvent(filterValue) {
  dataLayer.push({
    event: "click_date_filter_btn",
    filter_value: filterValue,
  });
}
