/**
 * Error Boundary Component
 * 
 * Graceful error handling for the entire app.
 * Catches React errors and displays user-friendly error page.
 */

import React from "react";
import "../styles/error-boundary.css";

interface ErrorBoundaryProps {
  children: React.ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends React.Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
    };
  }

  static getDerivedStateFromError(error: Error) {
    return {
      hasError: true,
      error,
    };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error("ErrorBoundary caught:", error, errorInfo);
  }

  handleReset = () => {
    this.setState({
      hasError: false,
      error: null,
    });
  };

  render() {
    if (this.state.hasError) {
      return (
        <div className="error-boundary">
          <div className="error-container">
            <h1>⚠️ Something went wrong</h1>
            <p className="error-message">
              {this.state.error?.message || "An unexpected error occurred"}
            </p>
            <button className="btn btn-primary" onClick={this.handleReset}>
              Retry
            </button>
            <a href="/" className="btn btn-secondary">
              Go Home
            </a>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
