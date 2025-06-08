import axios from 'axios';

// Determine base URL based on environment
// In a typical Next.js setup, you'd use environment variables like NEXT_PUBLIC_API_URL
// For now, hardcoding for localhost development with the orchestration service
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8083/api';
// Orchestration service is on port 8083 as per previous subtasks.

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// You can add interceptors for request/response handling here if needed
// For example, to automatically add auth tokens or handle errors globally

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    // Basic error logging. You could expand this to handle specific error codes,
    // redirect to login page for 401s, etc.
    console.error('API call error:', error.response?.status, error.response?.data || error.message);

    // It's good practice to not just swallow the error but to re-throw it
    // or a custom error object so that calling components can also handle it.
    return Promise.reject(error);
  }
);

export default apiClient;
