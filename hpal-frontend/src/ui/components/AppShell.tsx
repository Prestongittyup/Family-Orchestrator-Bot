import React from "react";
import { NavLink } from "react-router-dom";

interface AppShellProps {
  children: React.ReactNode;
}

export const AppShell: React.FC<AppShellProps> = ({ children }) => {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h1 className="brand">HPAL UI</h1>
        <nav className="nav-links" aria-label="Primary navigation">
          <NavLink to="/" end className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Dashboard
          </NavLink>
          <NavLink to="/tasks" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Tasks
          </NavLink>
          <NavLink to="/calendar" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Calendar
          </NavLink>
          <NavLink to="/chat" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Chat
          </NavLink>
        </nav>
      </aside>
      <main className="main-panel">{children}</main>
    </div>
  );
};
