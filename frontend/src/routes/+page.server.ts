import type { Actions } from '@sveltejs/kit';

export const actions = {
	getRecs: async ({ request }) => {
		const data = await request.formData();
		// call backend
		return { success: true };
	}
} satisfies Actions;
