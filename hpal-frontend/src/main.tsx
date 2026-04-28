import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { ErrorBoundary } from "./components/ErrorBoundary";
import "./ui/styles/app.css";

if (typeof window !== "undefined" && "serviceWorker" in navigator) {
  const disableServiceWorker = import.meta.env.DEV || window.location.hostname === "localhost";

  if (disableServiceWorker) {
    // Prevent stale SW caches from local builds from intercepting frontend reloads.
    navigator.serviceWorker.getRegistrations().then((registrations) => {
      for (const registration of registrations) {
        registration.unregister();
      }
    });

    if ("caches" in window) {
      caches.keys().then((keys) => {
        for (const key of keys) {
          if (key.startsWith("hpal-")) {
            caches.delete(key);
          }
        }
      });
    }
  } else {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("/sw.js", { scope: "/" }).catch((err) => {
        console.error("Service worker registration failed:", err);
      });
    });
  }
}

const rootElement = document.getElementById("root");

if (!rootElement) {
  throw new Error("Root element not found");
}

ReactDOM.createRoot(rootElement).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>
);
